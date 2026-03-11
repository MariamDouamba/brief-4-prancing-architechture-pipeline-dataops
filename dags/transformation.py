from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from psycopg2.extras import execute_values
import pandas as pd
import sys

sys.path.insert(0, "/usr/local/airflow/include")
from pipeline_config import (
    RAW_CSV, CLEAN_CSV, CHECKS_CLEAN,
    POSTGRES_CONN_ID
)
from soda_utils import run_soda_check


@dag(
    dag_id="transformation",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Déclenché par l'orchestrateur
    catchup=False,
    max_active_runs=1,
    tags=["dataops", "transformation"],
)
def transformation():

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

        # Extraction date et heure depuis "2026-03-01T20:30:00.000"
        dt = pd.to_datetime(df["date"], errors="coerce")
        df["date"] = dt.dt.date.astype(str)   # → "2026-03-01"
        df["hour"] = dt.dt.hour               # → 20 (entier 0-23)

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

    # Orchestration
    clean_count = transform_data()
    load_clean_to_postgres(clean_count) >> soda_check_clean()


transformation()
