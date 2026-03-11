"""
Utilitaire Soda partagé entre tous les DAGs.
- run_soda_check : lance un scan et sauvegarde les résultats en PostgreSQL
"""

import psycopg2
from datetime import datetime
from soda.scan import Scan

import sys
sys.path.insert(0, "/usr/local/airflow/include")
from pipeline_config import SODA_CONFIG, POSTGRES_CONN_ID


def _get_pg_conn():
    """Connexion directe PostgreSQL pour sauvegarder les résultats Soda."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()


def _ensure_results_table(cursor):
    """Crée la table soda_scan_results si elle n'existe pas."""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS soda_scan_results (
            id          SERIAL PRIMARY KEY,
            scan_time   TIMESTAMP NOT NULL,
            dataset     TEXT      NOT NULL,
            check_name  TEXT      NOT NULL,
            outcome     TEXT      NOT NULL,
            measured    TEXT
        );
    """)


def _save_results(scan, dataset: str):
    """Sauvegarde les résultats du scan dans soda_scan_results."""
    conn = _get_pg_conn()
    cursor = conn.cursor()
    _ensure_results_table(cursor)

    scan_time = datetime.utcnow()
    rows = []

    for check in scan._checks:
        outcome = str(check.outcome).split(".")[-1].lower()  # pass / fail / warn
        name = getattr(check, "name", None) or getattr(check.check_cfg, "source_line", "check")
        measured = str(check.get_measured_value()) if hasattr(check, "get_measured_value") else None
        rows.append((scan_time, dataset, name, outcome, measured))

    if rows:
        cursor.executemany(
            "INSERT INTO soda_scan_results (scan_time, dataset, check_name, outcome, measured) VALUES (%s,%s,%s,%s,%s)",
            rows
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Soda : {len(rows)} résultats sauvegardés pour '{dataset}'")


def run_soda_check(checks_file: str, dataset: str) -> None:
    """Lance un scan Soda, sauvegarde les résultats et lève une exception si des checks échouent."""
    scan = Scan()
    scan.set_scan_definition_name(dataset)
    scan.set_data_source_name("chicago_crimes_db")
    scan.add_configuration_yaml_file(file_path=SODA_CONFIG)
    scan.add_sodacl_yaml_file(file_path=checks_file)
    scan.execute()

    _save_results(scan, dataset)

    if scan.has_check_fails():
        raise ValueError(f"Soda checks failed for {dataset}:\n{scan.get_logs_text()}")
