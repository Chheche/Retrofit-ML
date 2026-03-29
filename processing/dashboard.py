"""
Dashboard DPE — Impact des classes energetiques sur la consommation
Etape 4 — Visualisation des resultats Gold

Usage :
    pip install -r requirements.txt
    streamlit run dashboard.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
import boto3
import json
import io
import os
from datetime import date

st.set_page_config(
    page_title="Analyse DPE — Gains Energetiques",
    page_icon="⚡",
    layout="wide",
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS",   "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET",   "minioadmin")
BUCKET         = "datalake"

DPE_COLORS = {
    "A": "#009A44",
    "B": "#51B849",
    "C": "#9DD532",
    "D": "#FFED00",
    "E": "#F7A400",
    "F": "#EE4023",
    "G": "#C11A25",
}


@st.cache_resource
def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
        region_name="us-east-1",
    )


@st.cache_data(ttl=300)
def load_gold_data():
    """Charge les fichiers Parquet depuis MinIO/gold via pandas."""
    s3 = get_s3()

    def read_parquet_from_minio(prefix):
        """Lit tous les fichiers parquet d'un dossier MinIO."""
        response = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        dfs = []
        for obj in response.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
                dfs.append(pd.read_parquet(io.BytesIO(body)))
        return pd.concat(dfs) if dfs else pd.DataFrame()

    stats   = read_parquet_from_minio("gold/stats_par_classe/")
    gains   = read_parquet_from_minio("gold/dashboard_gains/")
    matrix  = read_parquet_from_minio("gold/matrice_gains/")
    return stats, gains, matrix


@st.cache_data(ttl=300)
def load_silver_sample():
    """Charge un echantillon des donnees Silver pour les graphiques."""
    s3 = get_s3()
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix="silver/")
    dfs = []
    count = 0
    for obj in response.get("Contents", []):
        if obj["Key"].endswith(".parquet") and count < 5:
            body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
            dfs.append(pd.read_parquet(io.BytesIO(body)))
            count += 1
    return pd.concat(dfs) if dfs else pd.DataFrame()


# Header
st.title("⚡ Analyse DPE — Impact des classes energetiques")
st.markdown("**Source :** ADEME — DPE Logements existants (depuis juillet 2021)")
st.markdown("---")

# Chargement
with st.spinner("Chargement des donnees depuis MinIO..."):
    try:
        stats_df, gains_df, matrix_df = load_gold_data()
        silver_df = load_silver_sample()
        data_ok = not stats_df.empty
    except Exception as e:
        st.error(f"Erreur de connexion a MinIO : {e}")
        st.info("Verifie que Docker est lance et que le pipeline Gold a tourne.")
        data_ok = False

if not data_ok:
    st.warning("Aucune donnee Gold disponible. Lance d'abord silver_transform.py puis gold_analysis.py.")
    st.stop()

# KPIs en haut
st.subheader("Apercu global")
col1, col2, col3, col4 = st.columns(4)

total_logements = int(stats_df["nb_logements"].sum()) if "nb_logements" in stats_df.columns else 0
conso_moy_g     = float(stats_df[stats_df["Etiquette_DPE"] == "G"]["conso_moy_m2"].values[0]) if not stats_df[stats_df["Etiquette_DPE"] == "G"].empty else 0
conso_moy_a     = float(stats_df[stats_df["Etiquette_DPE"] == "A"]["conso_moy_m2"].values[0]) if not stats_df[stats_df["Etiquette_DPE"] == "A"].empty else 0
gain_total      = round(conso_moy_g - conso_moy_a, 0) if conso_moy_a else 0

col1.metric("Logements analyses", f"{total_logements:,}".replace(",", " "))
col2.metric("Conso moy. classe G", f"{conso_moy_g:.0f} kWh/m²/an")
col3.metric("Conso moy. classe A", f"{conso_moy_a:.0f} kWh/m²/an")
col4.metric("Gain G → A", f"-{gain_total:.0f} kWh/m²/an")

st.markdown("---")

# Graphique 1 : Conso moyenne par classe
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Consommation moyenne par classe DPE")
    if "Etiquette_DPE" in stats_df.columns and "conso_moy_m2" in stats_df.columns:
        df_sorted = stats_df.sort_values("Etiquette_DPE")
        fig, ax = plt.subplots(figsize=(7, 4))
        colors  = [DPE_COLORS.get(c, "#888") for c in df_sorted["Etiquette_DPE"]]
        bars    = ax.barh(df_sorted["Etiquette_DPE"], df_sorted["conso_moy_m2"], color=colors)
        ax.set_xlabel("kWh/m²/an")
        ax.set_title("Conso. moyenne par etiquette DPE")
        for bar, val in zip(bars, df_sorted["conso_moy_m2"]):
            ax.text(val + 2, bar.get_y() + bar.get_height()/2,
                    f"{val:.0f}", va="center", fontsize=9)
        ax.invert_yaxis()
        fig.tight_layout()
        st.pyplot(fig)
        plt.close()

# Graphique 2 : Gains par saut de classe
with col_right:
    st.subheader("Gain en kWh/m² par amelioration d'une classe")
    if not gains_df.empty and "saut" in gains_df.columns:
        fig2, ax2 = plt.subplots(figsize=(7, 4))
        gains_sorted = gains_df.sort_values("gain_kWh_m2", ascending=False)
        src_colors   = [DPE_COLORS.get(c, "#888") for c in gains_sorted["classe_origine"]]
        bars2 = ax2.bar(gains_sorted["saut"], gains_sorted["gain_kWh_m2"], color=src_colors)
        ax2.set_ylabel("kWh/m²/an economises")
        ax2.set_title("Gain par saut de classe (kWh/m²/an)")
        ax2.tick_params(axis="x", rotation=30)
        for bar, val in zip(bars2, gains_sorted["gain_kWh_m2"]):
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                     f"{val:.0f}", ha="center", fontsize=9)
        fig2.tight_layout()
        st.pyplot(fig2)
        plt.close()

st.markdown("---")

# Simulateur personnalise
st.subheader("🏠 Simulateur — Mon logement")
st.markdown("Estime les economies realisables en ameliorant la classe DPE de ton logement.")

sim_col1, sim_col2, sim_col3 = st.columns(3)
with sim_col1:
    surface = st.number_input("Surface habitable (m²)", min_value=9, max_value=500,
                               value=75, step=5)
with sim_col2:
    classe_actuelle = st.selectbox("Classe DPE actuelle",
                                   ["G", "F", "E", "D", "C", "B"])
with sim_col3:
    classes_cibles = ["A", "B", "C", "D", "E", "F"]
    options_cibles = [c for c in classes_cibles
                      if ["A","B","C","D","E","F","G"].index(c)
                       < ["A","B","C","D","E","F","G"].index(classe_actuelle)]
    if options_cibles:
        classe_cible = st.selectbox("Classe DPE cible", options_cibles)
    else:
        classe_cible = None
        st.info("Classe deja optimale !")

if classe_cible and not stats_df.empty:
    conso_src = stats_df[stats_df["Etiquette_DPE"] == classe_actuelle]["conso_moy_m2"]
    conso_tgt = stats_df[stats_df["Etiquette_DPE"] == classe_cible]["conso_moy_m2"]

    if not conso_src.empty and not conso_tgt.empty:
        gain_m2    = float(conso_src.values[0]) - float(conso_tgt.values[0])
        gain_total_kwh = gain_m2 * surface
        gain_euros = gain_total_kwh * 0.25   # ~0.25€/kWh (tarif indicatif)
        gain_co2   = gain_total_kwh * 0.035  # ~35g CO2/kWh mix FR

        r1, r2, r3 = st.columns(3)
        r1.metric(f"Gain {classe_actuelle} → {classe_cible}",
                  f"-{gain_m2:.0f} kWh/m²/an")
        r2.metric("Economie annuelle estimee",
                  f"-{gain_total_kwh:.0f} kWh/an",
                  f"≈ -{gain_euros:.0f} €/an")
        r3.metric("Reduction CO₂",
                  f"-{gain_co2:.0f} kg CO₂/an")

        # Visuel "passeport energetique"
        st.markdown("#### Passeport energetique")
        fig3, ax3 = plt.subplots(figsize=(8, 2))
        ax3.axis("off")

        classes_ordre = ["A", "B", "C", "D", "E", "F", "G"]
        for i, cls in enumerate(classes_ordre):
            alpha = 1.0 if cls in [classe_actuelle, classe_cible] else 0.3
            lw    = 3 if cls in [classe_actuelle, classe_cible] else 0.5
            rect  = mpatches.FancyBboxPatch(
                (i * 1.1, 0.1), 0.9, 0.8,
                boxstyle="round,pad=0.05",
                facecolor=DPE_COLORS[cls], alpha=alpha,
                linewidth=lw, edgecolor="white"
            )
            ax3.add_patch(rect)
            color_text = "white" if cls in ["F", "G"] else "black"
            ax3.text(i * 1.1 + 0.45, 0.7, cls,
                     ha="center", va="center", fontsize=14,
                     fontweight="bold", color=color_text, alpha=alpha)
            conso_val = stats_df[stats_df["Etiquette_DPE"] == cls]["conso_moy_m2"]
            if not conso_val.empty:
                ax3.text(i * 1.1 + 0.45, 0.3,
                         f"{conso_val.values[0]:.0f}",
                         ha="center", va="center", fontsize=7,
                         color=color_text, alpha=alpha)

        # Fleche entre les deux classes
        idx_src = classes_ordre.index(classe_actuelle)
        idx_tgt = classes_ordre.index(classe_cible)
        ax3.annotate("",
                     xy=(idx_tgt * 1.1 + 0.9, 0.5),
                     xytext=(idx_src * 1.1, 0.5),
                     arrowprops=dict(arrowstyle="->", color="black", lw=2))

        ax3.set_xlim(-0.1, 7.8)
        ax3.set_ylim(0, 1)
        fig3.tight_layout()
        st.pyplot(fig3)
        plt.close()

st.markdown("---")

# Distribution des consommations
if not silver_df.empty and "Conso_5_usages_par_m2_e_finale" in silver_df.columns:
    st.subheader("Distribution des consommations par classe DPE")
    fig4, ax4 = plt.subplots(figsize=(12, 4))
    for cls in ["A", "B", "C", "D", "E", "F", "G"]:
        subset = silver_df[silver_df["Etiquette_DPE"] == cls]["Conso_5_usages_par_m2_e_finale"]
        if len(subset) > 10:
            subset.plot.kde(ax=ax4, label=cls, color=DPE_COLORS[cls], linewidth=2)
    ax4.set_xlabel("kWh/m²/an")
    ax4.set_ylabel("Densite")
    ax4.set_xlim(0, 800)
    ax4.legend(title="Classe DPE")
    ax4.set_title("Distribution des consommations par classe")
    fig4.tight_layout()
    st.pyplot(fig4)
    plt.close()

# Footer
st.markdown("---")
st.caption(f"Donnees : ADEME — DPE Logements existants | Pipeline : Kafka → MinIO → Spark | {date.today()}")