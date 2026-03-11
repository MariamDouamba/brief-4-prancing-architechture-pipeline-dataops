from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import execute_values
import requests
import pandas as pd
import os
import sys
import time

sys.path.insert(0, "/usr/local/airflow/include")
from pipeline_config import (
    API_URL, API_BATCH_SIZE, MAX_ROWS,
    RAW_CSV, CLEAN_CSV, CHECKS_RAW, CHECKS_CLEAN,
    POSTGRES_CONN_ID, RAW_COLS
)
from soda_utils import run_soda_check


@dag(
    dag_id="orchestrateur",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["dataops", "orchestrateur"],
    doc_md="""
    ## Orchestrateur du pipeline Chicago Crimes

    Pipeline unifié avec TaskGroups :

    ```
    extraction
         ↓
    ┌────┴──────────────────┐
    transformation      profiling   ← parallèle
    └────┬──────────────────┘
         ↓
    chargement
    ```
    """,
)
def orchestrateur():

    # ── TaskGroup 1 : Extraction ──────────────────────────────────────
    with TaskGroup("extraction") as tg_extraction:

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

            def fetch_batch(offset):
                limit = min(API_BATCH_SIZE, total_to_fetch - offset)
                params = {"$limit": limit, "$offset": offset}
                wait = 30
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
                        wait *= 2
                return offset, []

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
                time.sleep(1)

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

        raw_count = ingest_data()
        load_raw_to_postgres(raw_count) >> soda_check_raw()

    # ── TaskGroup 2 : Transformation ─────────────────────────────────
    with TaskGroup("transformation") as tg_transformation:

        @task()
        def transform_data():
            """Nettoyage, typage et filtrage des données brutes."""
            df = pd.read_csv(RAW_CSV)

            cols = ["id", "date", "primary_type", "description",
                    "location_description", "arrest", "domestic",
                    "district", "year", "latitude", "longitude"]
            df = df[[c for c in cols if c in df.columns]].copy()

            df.rename(columns={"primary_type": "crime_type"}, inplace=True)
            df.dropna(subset=["id", "crime_type", "date"], inplace=True)
            df.drop_duplicates(subset=["id"], inplace=True)

            dt = pd.to_datetime(df["date"], errors="coerce")
            df["date"] = dt.dt.date.astype(str)
            df["hour"] = dt.dt.hour

            df["year"] = pd.to_numeric(df["year"], errors="coerce")
            df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
            df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
            df["arrest"] = df["arrest"].map({"true": True, "false": False,
                                              "True": True, "False": False})
            df = df[(df["year"] >= 2000) & (df["year"] <= 2025)]
            df["crime_type"] = df["crime_type"].str.strip().str.upper()

            df.to_csv(CLEAN_CSV, index=False)
            print(f"Transformation OK : {len(df)} lignes dans {CLEAN_CSV}")
            return len(df)

        @task()
        def load_clean_to_postgres(row_count: int):
            """Charge les données transformées en base via bulk insert."""
            df = pd.read_csv(CLEAN_CSV)

            def to_bool(val):
                if pd.isna(val) or val is None:
                    return None
                return True if str(val).strip().lower() in ("true", "1") else (
                    False if str(val).strip().lower() in ("false", "0") else None
                )

            if "arrest" in df.columns:
                df["arrest"] = df["arrest"].apply(to_bool)
            df = df.where(pd.notnull(df), None)

            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            cursor = conn.cursor()

            cursor.execute("DROP TABLE IF EXISTS chicago_crimes_clean CASCADE;")
            cursor.execute("""
                CREATE TABLE chicago_crimes_clean (
                    id TEXT, date DATE, hour SMALLINT, crime_type TEXT, description TEXT,
                    location_description TEXT, arrest BOOLEAN, domestic TEXT,
                    district TEXT, year INTEGER, latitude FLOAT, longitude FLOAT
                );
            """)

            clean_cols = ["id", "date", "hour", "crime_type", "description",
                          "location_description", "arrest", "domestic",
                          "district", "year", "latitude", "longitude"]
            present_cols = [c for c in clean_cols if c in df.columns]
            execute_values(
                cursor,
                f"INSERT INTO chicago_crimes_clean ({', '.join(present_cols)}) VALUES %s",
                list(df[present_cols].itertuples(index=False, name=None)),
                page_size=1000
            )
            conn.commit()
            cursor.close()
            conn.close()
            print(f"Chargement clean OK : {len(df)} lignes dans chicago_crimes_clean")

        @task()
        def soda_check_clean():
            """Contrôle qualité Soda sur les données transformées."""
            run_soda_check(CHECKS_CLEAN, "chicago_crimes_clean")
            print("Soda check CLEAN : OK")

        clean_count = transform_data()
        load_clean_to_postgres(clean_count) >> soda_check_clean()

    # ── TaskGroup 3 : Profiling (parallèle avec transformation) ──────
    with TaskGroup("profiling") as tg_profiling:

        @task()
        def profiling_raw():
            """Génère un rapport de statistiques sur les données brutes."""
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM chicago_crimes_raw")
            total = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM chicago_crimes_raw WHERE id IS NULL")
            nulls_id = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM chicago_crimes_raw WHERE date IS NULL")
            nulls_date = cursor.fetchone()[0]
            cursor.execute("""
                SELECT primary_type, COUNT(*) as nb
                FROM chicago_crimes_raw
                GROUP BY primary_type
                ORDER BY nb DESC
                LIMIT 5
            """)
            top5 = cursor.fetchall()

            cursor.close()
            conn.close()

            print("=" * 50)
            print("PROFILING DONNÉES BRUTES")
            print("=" * 50)
            print(f"Total lignes      : {total:,}")
            print(f"ID nulls          : {nulls_id}")
            print(f"Date nulls        : {nulls_date}")
            print("Top 5 types de crimes :")
            for crime_type, nb in top5:
                print(f"  {crime_type:<30} {nb:>8,}")
            print("=" * 50)

        profiling_raw()

    # ── TaskGroup 4 : Chargement final ────────────────────────────────
    with TaskGroup("chargement") as tg_chargement:

        @task()
        def load_final_to_postgres():
            """Renomme chicago_crimes_clean en table de production finale."""
            hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = hook.get_conn()
            cursor = conn.cursor()

            cursor.execute("DROP TABLE IF EXISTS chicago_crimes_final;")
            cursor.execute("ALTER TABLE chicago_crimes_clean RENAME TO chicago_crimes_final;")

            conn.commit()
            cursor.close()
            conn.close()
            print("Chargement final OK : table chicago_crimes_final prête.")

        load_final_to_postgres()

    # ── Orchestration avec parallélisation ───────────────────────────
    tg_extraction >> [tg_transformation, tg_profiling] >> tg_chargement


orchestrateur()
