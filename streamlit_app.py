"""
Dashboard Streamlit — Rapports qualité Soda
Visualise les résultats des scans Soda stockés dans PostgreSQL.

Lancement :
    streamlit run streamlit_app.py
"""

import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ── Configuration ─────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Soda Quality Dashboard",
    page_icon="✅",
    layout="wide",
)

PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "postgres",
}

# ── Connexion PostgreSQL ───────────────────────────────────────────────────────

@st.cache_resource
def get_connection():
    return psycopg2.connect(**PG_CONFIG)


@st.cache_data(ttl=30)
def load_results() -> pd.DataFrame:
    try:
        conn = get_connection()
        df = pd.read_sql(
            "SELECT * FROM soda_scan_results ORDER BY scan_time DESC",
            conn
        )
        df["scan_time"] = pd.to_datetime(df["scan_time"])
        return df
    except Exception as e:
        return pd.DataFrame()


@st.cache_data(ttl=30)
def load_crimes_stats() -> dict:
    try:
        conn = get_connection()
        stats = {}

        stats["total_final"] = pd.read_sql(
            "SELECT COUNT(*) as n FROM chicago_crimes_final", conn
        ).iloc[0]["n"]

        stats["top_crimes"] = pd.read_sql("""
            SELECT crime_type, COUNT(*) as nb
            FROM chicago_crimes_final
            GROUP BY crime_type
            ORDER BY nb DESC
            LIMIT 10
        """, conn)

        stats["crimes_by_year"] = pd.read_sql("""
            SELECT year, COUNT(*) as nb
            FROM chicago_crimes_final
            WHERE year IS NOT NULL
            GROUP BY year
            ORDER BY year
        """, conn)

        stats["arrest_rate"] = pd.read_sql("""
            SELECT
                COUNT(*) FILTER (WHERE arrest = true) as arrested,
                COUNT(*) as total
            FROM chicago_crimes_final
        """, conn)

        return stats
    except Exception:
        return {}


# ── UI ────────────────────────────────────────────────────────────────────────

st.title("Dashboard Qualité — Chicago Crimes Pipeline")
st.caption("Rapports Soda + Statistiques des données • Rafraîchi toutes les 30s")

df = load_results()
stats = load_crimes_stats()

# ── KPIs données ──────────────────────────────────────────────────────────────

if stats:
    col1, col2, col3, col4 = st.columns(4)

    total = int(stats.get("total_final", 0))
    col1.metric("Lignes en production", f"{total:,}")

    arrest_df = stats.get("arrest_rate", pd.DataFrame())
    if not arrest_df.empty:
        rate = arrest_df.iloc[0]["arrested"] / arrest_df.iloc[0]["total"] * 100
        col2.metric("Taux d'arrestation", f"{rate:.1f}%")

    if not df.empty:
        last_scan = df["scan_time"].max()
        col3.metric("Dernier scan", last_scan.strftime("%d/%m %H:%M"))
        total_scans = df["scan_time"].nunique()
        col4.metric("Scans effectués", total_scans)

st.divider()

# ── Résultats Soda ────────────────────────────────────────────────────────────

st.header("Contrôles qualité Soda")

if df.empty:
    st.warning("Aucun résultat Soda trouvé. Lancez le pipeline pour générer des données.")
else:
    # Sélecteur de scan
    scans = df["scan_time"].dt.strftime("%Y-%m-%d %H:%M:%S").unique().tolist()
    selected_scan = st.selectbox("Sélectionner un scan", scans)

    scan_df = df[df["scan_time"].dt.strftime("%Y-%m-%d %H:%M:%S") == selected_scan]

    # KPIs qualité
    c1, c2, c3 = st.columns(3)
    n_pass = (scan_df["outcome"] == "pass").sum()
    n_fail = (scan_df["outcome"] == "fail").sum()
    n_warn = (scan_df["outcome"] == "warn").sum()
    c1.metric("✅ Checks passés", n_pass)
    c2.metric("❌ Checks échoués", n_fail)
    c3.metric("⚠️ Avertissements", n_warn)

    col_left, col_right = st.columns([1, 2])

    # Pie chart pass/fail
    with col_left:
        pie_df = scan_df["outcome"].value_counts().reset_index()
        pie_df.columns = ["outcome", "count"]
        colors = {"pass": "#2ecc71", "fail": "#e74c3c", "warn": "#f39c12"}
        fig_pie = px.pie(
            pie_df, names="outcome", values="count",
            color="outcome", color_discrete_map=colors,
            title="Répartition pass / fail / warn"
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    # Tableau des checks
    with col_right:
        st.subheader("Détail des checks")
        display_df = scan_df[["dataset", "check_name", "outcome", "measured"]].copy()
        display_df["outcome"] = display_df["outcome"].map({
            "pass": "✅ pass",
            "fail": "❌ fail",
            "warn": "⚠️ warn"
        }).fillna(scan_df["outcome"])
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    # Historique
    st.subheader("Historique des scans")
    hist = df.groupby(["scan_time", "outcome"]).size().reset_index(name="count")
    fig_hist = px.bar(
        hist, x="scan_time", y="count", color="outcome",
        color_discrete_map={"pass": "#2ecc71", "fail": "#e74c3c", "warn": "#f39c12"},
        title="Résultats par scan dans le temps",
        labels={"scan_time": "Date du scan", "count": "Nombre de checks"}
    )
    st.plotly_chart(fig_hist, use_container_width=True)

st.divider()

# ── Statistiques des données ──────────────────────────────────────────────────

st.header("Statistiques des données")

if stats:
    col_a, col_b = st.columns(2)

    # Top 10 types de crimes
    with col_a:
        top = stats.get("top_crimes", pd.DataFrame())
        if not top.empty:
            fig_crimes = px.bar(
                top, x="nb", y="crime_type", orientation="h",
                title="Top 10 types de crimes",
                labels={"nb": "Nombre", "crime_type": "Type"},
                color="nb", color_continuous_scale="Reds"
            )
            fig_crimes.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_crimes, use_container_width=True)

    # Crimes par année
    with col_b:
        by_year = stats.get("crimes_by_year", pd.DataFrame())
        if not by_year.empty:
            fig_year = px.line(
                by_year, x="year", y="nb",
                title="Évolution des crimes par année",
                labels={"nb": "Nombre de crimes", "year": "Année"},
                markers=True
            )
            st.plotly_chart(fig_year, use_container_width=True)
else:
    st.info("Lancez le pipeline pour afficher les statistiques.")

# ── Footer ────────────────────────────────────────────────────────────────────

st.divider()
st.caption("Pipeline DataOps — Chicago Crimes | Simplon Formation | Airflow + Soda + Streamlit")
