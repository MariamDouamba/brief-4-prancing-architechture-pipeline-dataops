from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import execute_values
import requests
import pandas as pd
import os
import sys

sys.path.insert(0, "/usr/local/airflow/include")
from pipeline_config import (
    API_URL, API_BATCH_SIZE, MAX_ROWS,
    RAW_CSV, CHECKS_RAW,
    POSTGRES_CONN_ID, RAW_COLS
)
from soda_utils import run_soda_check


@dag(
    dag_id="extraction",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Déclenché par l'orchestrateur
    catchup=False,
    max_active_runs=1,
    tags=["dataops", "extraction"],
)
def extraction():

    @task()
    def ingest_data():
        """Récupère toutes les données en parallèle depuis l'API Chicago."""
        os.makedirs(os.path.dirname(RAW_CSV), exist_ok=True)

        count_resp = requests.get(
            API_URL, params={"$select": "count(*)", "$limit": 1}, timeout=30
        )
        count_resp.raise_for_status()
        total_available = int(count_resp.json()[0]["count"])
        total_to_fetch = min(total_available, MAX_ROWS) if MAX_ROWS else total_available
        offsets = list(range(0, total_to_fetch, API_BATCH_SIZE))
        print(f"Total disponible : {total_available} | À récupérer : {total_to_fetch} | Lots : {len(offsets)}")

        import time

        def fetch_batch(offset):
            limit = min(API_BATCH_SIZE, total_to_fetch - offset)
            params = {"$limit": limit, "$offset": offset}
            wait = 30  # secondes entre retries (backoff)
            for attempt in range(1, 4):
                try:
                    resp = requests.get(API_URL, params=params, timeout=120)
                    resp.raise_for_status()
                    batch = resp.json()
                    print(f"  Lot récupéré : {len(batch)} lignes (offset={offset})")
                    return offset, batch
                except requests.exceptions.Timeout:
                    print(f"  Timeout offset={offset} tentative {attempt}/3, attente {wait}s...")
                    if attempt == 3:
                        raise
                    time.sleep(wait)
                    wait *= 2  # 30s → 60s
            return offset, []

        # 5 workers pour accélérer l'ingestion
        WINDOW = 5
        total_written = 0
        first = True

        for i in range(0, len(offsets), WINDOW):
            window_offsets = offsets[i:i + WINDOW]

            with ThreadPoolExecutor(max_workers=WINDOW) as executor:
                futures = {executor.submit(fetch_batch, off): off for off in window_offsets}
                window_results = {}
                for future in as_completed(futures):
                    offset, batch = future.result()
                    window_results[offset] = batch

            # Écriture ordonnée puis libération immédiate
            for offset in sorted(window_results.keys()):
                batch = window_results[offset]
                if not batch:
                    continue
                df = pd.DataFrame(batch)
                df.to_csv(RAW_CSV, mode="w" if first else "a", header=first, index=False)
                total_written += len(df)
                first = False
                del df

            del window_results
            print(f"Fenêtre {i // WINDOW + 1} traitée — total écrit : {total_written:,}")
            time.sleep(1)  # pause légère entre fenêtres

        print(f"Ingestion terminée : {total_written} lignes dans {RAW_CSV}")
        return total_written

    @task()
    def load_raw_to_postgres(row_count: int):
        """Charge les données brutes en base via bulk insert."""
        df = pd.read_csv(RAW_CSV)
        cols = [c for c in RAW_COLS if c in df.columns]
        df = df[cols].where(pd.notnull(df[cols]), None)

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("DROP TABLE IF EXISTS chicago_crimes_raw CASCADE;")
        cursor.execute(f"CREATE TABLE chicago_crimes_raw ({', '.join(f'{c} TEXT' for c in cols)});")

        execute_values(
            cursor,
            f"INSERT INTO chicago_crimes_raw ({', '.join(cols)}) VALUES %s",
            list(df.itertuples(index=False, name=None)),
            page_size=1000
        )
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Chargement brut OK : {len(df)} lignes dans chicago_crimes_raw")

    @task()
    def soda_check_raw():
        """Contrôle qualité Soda sur les données brutes."""
        run_soda_check(CHECKS_RAW, "chicago_crimes_raw")
        print("Soda check RAW : OK")

    # Orchestration
    raw_count = ingest_data()
    load_raw_to_postgres(raw_count) >> soda_check_raw()


extraction()
