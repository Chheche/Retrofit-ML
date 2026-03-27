import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from minio import Minio
import io
import numpy as np

st.set_page_config(
    page_title="Enedis — DPE Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&display=swap');

html, body, [class*="css"] {
    font-family: 'Outfit', sans-serif !important;
    background-color: #0a1628 !important;
    color: #e8f4fd !important;
}

.stApp { background-color: #0a1628 !important; }

section[data-testid="stSidebar"] { display: none; }

.block-container {
    padding: 0 !important;
    max-width: 100% !important;
}

.enedis-header {
    background: #0d1f3c;
    border-bottom: 2px solid #00a3e0;
    padding: 16px 40px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}

.enedis-logo {
    background: #00a3e0;
    color: white;
    font-weight: 700;
    font-size: 20px;
    padding: 6px 16px;
    border-radius: 6px;
    letter-spacing: 2px;
    display: inline-block;
}

.kpi-card {
    background: #0d1f3c;
    border: 1px solid rgba(0,163,224,0.4);
    border-radius: 12px;
    padding: 20px 24px;
    border-top: 2px solid #00a3e0;
    margin-bottom: 4px;
}

.kpi-card.green { border-top-color: #00c864; }
.kpi-card.orange { border-top-color: #ff8c00; }
.kpi-card.red { border-top-color: #ff4757; }

.kpi-label {
    font-size: 11px;
    color: #7ab3d4;
    letter-spacing: 1px;
    text-transform: uppercase;
    margin-bottom: 8px;
}

.kpi-value {
    font-size: 30px;
    font-weight: 700;
    color: #e8f4fd;
    line-height: 1.1;
}

.kpi-unit {
    font-size: 13px;
    color: #7ab3d4;
    font-weight: 400;
    margin-left: 4px;
}

.kpi-delta-green { font-size: 12px; color: #00c864; margin-top: 6px; }
.kpi-delta-orange { font-size: 12px; color: #ff8c00; margin-top: 6px; }

.chart-card {
    background: #0d1f3c;
    border: 1px solid rgba(0,163,224,0.4);
    border-radius: 12px;
    padding: 24px;
    margin-bottom: 20px;
}

.chart-title {
    font-size: 14px;
    font-weight: 600;
    color: #e8f4fd;
    margin-bottom: 4px;
}

.chart-subtitle {
    font-size: 11px;
    color: #7ab3d4;
    margin-bottom: 16px;
}

.section-title {
    font-size: 11px;
    font-weight: 600;
    color: #00a3e0;
    letter-spacing: 2px;
    text-transform: uppercase;
    padding: 24px 40px 12px;
}

.result-metric {
    text-align: center;
    padding: 16px;
    background: rgba(0,163,224,0.06);
    border: 1px solid rgba(0,163,224,0.3);
    border-radius: 8px;
}

.result-val {
    font-size: 28px;
    font-weight: 700;
}

.result-lbl {
    font-size: 11px;
    color: #7ab3d4;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-top: 4px;
}

div[data-baseweb="tab-list"] {
    background: #0d1f3c !important;
    border-bottom: 1px solid rgba(0,163,224,0.15) !important;
    padding: 0 40px !important;
    gap: 0 !important;
}

div[data-baseweb="tab"] {
    background: transparent !important;
    color: #7ab3d4 !important;
    font-family: 'Outfit', sans-serif !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    letter-spacing: 1px !important;
    text-transform: uppercase !important;
    padding: 14px 40px !important;
    border-bottom: 2px solid transparent !important;
}

div[data-baseweb="tab"][aria-selected="true"] {
    color: #00a3e0 !important;
    border-bottom: 2px solid #00a3e0 !important;
}

div[data-baseweb="tab-panel"] {
    background: transparent !important;
    padding: 0 !important;
}

div[data-baseweb="select"] > div {
    background: #0a1628 !important;
    border-color: rgba(0,163,224,0.3) !important;
    color: #e8f4fd !important;
    border-radius: 8px !important;
}

div.stButton > button {
    background: #00a3e0 !important;
    color: white !important;
    border: none !important;
    border-radius: 8px !important;
    font-family: 'Outfit', sans-serif !important;
    font-weight: 600 !important;
    font-size: 13px !important;
    letter-spacing: 1px !important;
    text-transform: uppercase !important;
    padding: 14px 28px !important;
    width: 100% !important;
}

div.stButton > button:hover { background: #0088c0 !important; }

input[type="number"] {
    background: #0a1628 !important;
    color: #e8f4fd !important;
    border-color: rgba(0,163,224,0.3) !important;
    border-radius: 8px !important;
}
</style>
""", unsafe_allow_html=True)

# =============================================================
# CONNEXION MINIO
# =============================================================
@st.cache_resource
def get_minio_client():
    return Minio(
        "localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

@st.cache_data
def load_predictions():
    try:
        client = get_minio_client()
        objects = list(client.list_objects(
            "datalake", prefix="gold/predictions/", recursive=True
        ))
        dfs = []
        for obj in objects:
            if obj.object_name.endswith(".parquet"):
                data = client.get_object("datalake", obj.object_name)
                df = pd.read_parquet(io.BytesIO(data.read()))
                dfs.append(df)
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    except Exception as e:
        st.error(f"Erreur MinIO : {e}")
        return pd.DataFrame()

# =============================================================
# PLOTLY LAYOUT DE BASE
# =============================================================
PLOTLY_LAYOUT = dict(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(13,31,60,0.5)',
    font=dict(family='Outfit', color='#7ab3d4', size=12),
    title_font=dict(color='#e8f4fd', size=14),
    xaxis=dict(gridcolor='rgba(255,255,255,0.04)', color='#7ab3d4'),
    yaxis=dict(gridcolor='rgba(255,255,255,0.04)', color='#7ab3d4'),
    margin=dict(l=20, r=20, t=40, b=20),
)

# =============================================================
# HEADER
# =============================================================
st.markdown("""
<div class="enedis-header">
    <div style="display:flex;align-items:center;gap:16px;">
        <div class="enedis-logo">ENEDIS</div>
        <div>
            <div style="font-size:16px;font-weight:600;color:#e8f4fd;">
                Plateforme DPE Analytics
            </div>
            <div style="font-size:11px;color:#7ab3d4;letter-spacing:2px;text-transform:uppercase;">
                Diagnostic de Performance Énergétique
            </div>
        </div>
    </div>
    <div style="display:flex;align-items:center;gap:16px;">
        <div style="background:rgba(0,200,100,0.15);border:1px solid #00c864;color:#00c864;
                    font-size:11px;font-weight:600;padding:4px 10px;border-radius:20px;
                    letter-spacing:1px;text-transform:uppercase;">Live</div>
        <div style="background:rgba(0,163,224,0.15);border:1px solid rgba(0,163,224,0.4);
                    color:#00a3e0;font-size:12px;padding:6px 14px;border-radius:6px;">
            Nadia Bouaicha
        </div>
    </div>
</div>
""", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["Dashboard", "Simulateur", "Modèle ML"])

# =============================================================
# TAB 1 — DASHBOARD
# =============================================================
with tab1:
    st.markdown('<div class="section-title">Vue d\'ensemble — Analyse DPE France</div>',
                unsafe_allow_html=True)

    with st.spinner("Chargement des données..."):
        df = load_predictions()

    if df.empty:
        st.warning("Données non disponibles — affichage des données de démonstration")
        np.random.seed(42)
        n = 5000
        etiquettes = np.random.choice(
            ['A','B','C','D','E','F','G'],
            size=n, p=[0.03,0.08,0.18,0.28,0.22,0.13,0.06]
        )
        conso_map = {'A':50,'B':80,'C':150,'D':230,'E':330,'F':450,'G':600}
        conso_reelle = np.array([conso_map[e] + np.random.normal(0,30) for e in etiquettes])
        conso_reelle = np.clip(conso_reelle, 10, 700)
        prediction = conso_reelle + np.random.normal(0, 29, n)
        prediction = np.clip(prediction, 10, 700)
        df = pd.DataFrame({
            'etiquette_dpe': etiquettes,
            'conso_5_usages_par_m2_ef': conso_reelle,
            'prediction': prediction,
            'zone_climatique': np.random.choice(
                ['H1a','H1b','H1c','H2a','H2b','H2c','H2d','H3'], size=n
            ),
            'nom_commune_ban': np.random.choice(
                ['Paris','Lyon','Marseille','Lille','Strasbourg',
                 'Maubeuge','Roubaix','Dunkerque','Lens','Nancy'], size=n
            ),
            'type_batiment': np.random.choice(['maison','appartement','immeuble'], size=n),
            'surface_habitable_immeuble': np.random.uniform(30, 200, n),
        })

    rmse = float(np.sqrt(((df['conso_5_usages_par_m2_ef'] - df['prediction'])**2).mean()))
    conso_moy = float(df['conso_5_usages_par_m2_ef'].mean())

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-label">Logements analysés</div>
            <div class="kpi-value">{len(df):,}<span class="kpi-unit">logements</span></div>
            <div class="kpi-delta-green">↑ Pipeline complet traité</div>
        </div>""", unsafe_allow_html=True)
    with col2:
        st.markdown(f"""<div class="kpi-card green">
            <div class="kpi-label">Consommation moyenne</div>
            <div class="kpi-value">{conso_moy:.0f}<span class="kpi-unit">kWh/m²/an</span></div>
            <div class="kpi-delta-orange">Objectif &lt; 150 kWh/m²</div>
        </div>""", unsafe_allow_html=True)
    with col3:
        st.markdown(f"""<div class="kpi-card orange">
            <div class="kpi-label">RMSE modèle GBT</div>
            <div class="kpi-value">{rmse:.2f}<span class="kpi-unit">kWh/m²</span></div>
            <div class="kpi-delta-green">↑ Meilleur que RF (30.31)</div>
        </div>""", unsafe_allow_html=True)
    with col4:
        st.markdown(f"""<div class="kpi-card">
            <div class="kpi-label">Score R²</div>
            <div class="kpi-value">0.889</div>
            <div class="kpi-delta-green">↑ 89% variance expliquée</div>
        </div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Distribution des étiquettes DPE</div>',
                    unsafe_allow_html=True)
        st.markdown('<div class="chart-subtitle">Répartition par classe énergétique</div>',
                    unsafe_allow_html=True)
        dpe_counts = df['etiquette_dpe'].value_counts().reset_index()
        dpe_counts.columns = ['etiquette', 'count']
        dpe_counts = dpe_counts.sort_values('etiquette')
        colors_dpe = {
            'A':'#00A651','B':'#50B848','C':'#C8D400',
            'D':'#FFCC00','E':'#F7A600','F':'#EE7203','G':'#E52222'
        }
        fig1 = px.bar(
            dpe_counts, x='etiquette', y='count',
            color='etiquette', color_discrete_map=colors_dpe,
        )
        fig1.update_layout(**PLOTLY_LAYOUT, showlegend=False, height=280)
        fig1.update_traces(marker_line_width=0)
        st.plotly_chart(fig1, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col_right:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Consommation par zone climatique</div>',
                    unsafe_allow_html=True)
        st.markdown('<div class="chart-subtitle">Moyenne en kWh/m²/an</div>',
                    unsafe_allow_html=True)
        if 'zone_climatique' in df.columns:
            zone_conso = df.groupby('zone_climatique')['conso_5_usages_par_m2_ef'] \
                .mean().reset_index()
            zone_conso.columns = ['zone', 'conso']
            zone_conso = zone_conso.sort_values('conso')
        else:
            zone_conso = pd.DataFrame({
                'zone': ['H1a','H1b','H1c','H2a','H2b','H2c','H2d','H3'],
                'conso': [210, 198, 205, 175, 168, 172, 155, 140]
            })
        fig2 = px.bar(
            zone_conso, x='conso', y='zone', orientation='h',
            color='conso', color_continuous_scale=['#00a3e0', '#ff4757'],
        )
        fig2.update_layout(**PLOTLY_LAYOUT, coloraxis_showscale=False, height=280)
        st.plotly_chart(fig2, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">Réel vs Prédit — Modèle GBT</div>',
                unsafe_allow_html=True)
    st.markdown(
        '<div class="chart-subtitle">Comparaison consommation réelle et prédiction (kWh/m²/an)</div>',
        unsafe_allow_html=True
    )
    sample = df.sample(min(800, len(df)), random_state=42)
    max_val = float(max(
        sample['conso_5_usages_par_m2_ef'].max(),
        sample['prediction'].max()
    ))
    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(
        x=sample['conso_5_usages_par_m2_ef'],
        y=sample['prediction'],
        mode='markers',
        marker=dict(color='rgba(0,163,224,0.4)', size=5),
        name='Prédictions'
    ))
    fig3.add_trace(go.Scatter(
        x=[0, max_val], y=[0, max_val],
        mode='lines',
        line=dict(color='#00c864', dash='dash', width=2),
        name='Prédiction parfaite'
    ))
    fig3.update_layout(
        **PLOTLY_LAYOUT, height=350,
        legend=dict(font=dict(color='#7ab3d4'))
    )
    st.plotly_chart(fig3, use_container_width=True)
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">Top 10 communes les plus énergivores</div>',
                unsafe_allow_html=True)
    st.markdown('<div class="chart-subtitle">Consommation moyenne en kWh/m²/an</div>',
                unsafe_allow_html=True)
    if 'nom_commune_ban' in df.columns:
        top_communes = df.groupby('nom_commune_ban')['conso_5_usages_par_m2_ef'] \
            .mean().reset_index() \
            .sort_values('conso_5_usages_par_m2_ef', ascending=False) \
            .head(10)
        fig4 = px.bar(
            top_communes,
            x='conso_5_usages_par_m2_ef',
            y='nom_commune_ban',
            orientation='h',
            color='conso_5_usages_par_m2_ef',
            color_continuous_scale=['#00a3e0', '#ff4757'],
        )
        fig4.update_layout(
            **PLOTLY_LAYOUT,
            yaxis={'categoryorder': 'total ascending'},
            coloraxis_showscale=False,
            height=350
        )
        st.plotly_chart(fig4, use_container_width=True)
    else:
        st.info("Colonne nom_commune_ban non disponible dans les données gold")
    st.markdown('</div>', unsafe_allow_html=True)

# =============================================================
# TAB 2 — SIMULATEUR
# =============================================================
with tab2:
    st.markdown('<div class="section-title">Simulateur de rénovation énergétique</div>',
                unsafe_allow_html=True)

    conso_par_etiquette = {
        'A': 50, 'B': 80, 'C': 150,
        'D': 230, 'E': 330, 'F': 450, 'G': 600
    }

    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown(
            '<div class="chart-title" style="margin-bottom:20px;">Caractéristiques du logement</div>',
            unsafe_allow_html=True
        )
        dpe_actuel = st.selectbox(
            "Étiquette DPE actuelle",
            ['G', 'F', 'E', 'D', 'C', 'B'],
            index=2
        )
        dpe_cible = st.selectbox(
            "Étiquette DPE cible (après rénovation)",
            ['A', 'B', 'C', 'D'],
            index=1
        )
        surface = st.number_input("Surface habitable (m²)", 10, 500, 80)
        prix_kwh = st.number_input("Prix du kWh (€)", 0.10, 0.50, 0.23, step=0.01)
        cout_travaux = st.number_input(
            "Coût estimé des travaux (€)", 1000, 100000, 15000, step=1000
        )
        calculer = st.button("Calculer les économies")
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown(
            '<div class="chart-card" style="min-height:400px;">',
            unsafe_allow_html=True
        )
        st.markdown(
            '<div class="chart-title" style="margin-bottom:20px;">Résultats de la simulation</div>',
            unsafe_allow_html=True
        )

        if calculer:
            c_av = conso_par_etiquette[dpe_actuel]
            c_ap = conso_par_etiquette[dpe_cible]

            if c_ap >= c_av:
                st.error("L'étiquette cible doit être meilleure que l'étiquette actuelle.")
            else:
                econ_kwh = (c_av - c_ap) * surface
                econ_eur = econ_kwh * prix_kwh
                amort = cout_travaux / econ_eur
                pct = round((c_av - c_ap) / c_av * 100)

                r1, r2, r3 = st.columns(3)
                with r1:
                    st.markdown(f"""<div class="result-metric">
                        <div class="result-val" style="color:#ff8c00;">{c_av}</div>
                        <div style="font-size:11px;color:#7ab3d4;">kWh/m²/an</div>
                        <div class="result-lbl">Avant</div>
                    </div>""", unsafe_allow_html=True)
                with r2:
                    st.markdown(f"""<div class="result-metric">
                        <div class="result-val" style="color:#00c864;">{c_ap}</div>
                        <div style="font-size:11px;color:#7ab3d4;">kWh/m²/an</div>
                        <div class="result-lbl">Après</div>
                    </div>""", unsafe_allow_html=True)
                with r3:
                    st.markdown(f"""<div class="result-metric">
                        <div class="result-val" style="color:#00a3e0;">{round(econ_eur):,}€</div>
                        <div style="font-size:11px;color:#7ab3d4;">par an</div>
                        <div class="result-lbl">Économie</div>
                    </div>""", unsafe_allow_html=True)

                st.markdown("<br>", unsafe_allow_html=True)

                fig_sim = go.Figure(go.Bar(
                    x=['Avant rénovation', 'Après rénovation'],
                    y=[int(c_av * surface), int(c_ap * surface)],
                    marker_color=['#ff4757', '#00c864'],
                    text=[
                        f"{int(c_av * surface):,} kWh/an",
                        f"{int(c_ap * surface):,} kWh/an"
                    ],
                    textposition='outside',
                    textfont=dict(color='#e8f4fd', size=12),
                    name=''
                ))
                fig_sim.update_layout(
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(13,31,60,0.5)',
                    font=dict(family='Outfit', color='#7ab3d4', size=12),
                    xaxis=dict(
                        gridcolor='rgba(255,255,255,0.04)',
                        color='#7ab3d4'
                    ),
                    yaxis=dict(
                        gridcolor='rgba(255,255,255,0.04)',
                        color='#7ab3d4',
                        ticksuffix=' kWh'
                    ),
                    margin=dict(l=20, r=20, t=40, b=20),
                    height=250,
                    showlegend=False,
                    title=None,
                )
                st.plotly_chart(fig_sim, use_container_width=True)

                st.markdown(f"""
                <div style="background:rgba(0,200,100,0.08);
                            border:1px solid rgba(0,200,100,0.3);
                            border-radius:8px;padding:16px;margin-top:12px;">
                    <div style="font-size:13px;color:#e8f4fd;">
                        DPE <strong style="color:#ff8c00;">{dpe_actuel}</strong> →
                        <strong style="color:#00c864;">{dpe_cible}</strong> :
                        réduction de <strong style="color:#00c864;">{pct}%</strong> —
                        économie de
                        <strong style="color:#00a3e0;">{round(econ_eur):,}€/an</strong> —
                        amortissement en
                        <strong style="color:#00c864;">{amort:.1f} ans</strong>
                    </div>
                </div>""", unsafe_allow_html=True)
        else:
            st.markdown("""
            <div style="text-align:center;padding:80px 0;color:#7ab3d4;font-size:14px;">
                Renseignez les paramètres et cliquez sur<br>Calculer les économies
            </div>""", unsafe_allow_html=True)

        st.markdown('</div>', unsafe_allow_html=True)

# =============================================================
# TAB 3 — MODELE ML
# =============================================================
with tab3:
    st.markdown('<div class="section-title">Détails du modèle Machine Learning</div>',
                unsafe_allow_html=True)

    col1, col2 = st.columns(2)

    with col1:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Comparaison des modèles</div>',
                    unsafe_allow_html=True)
        st.markdown('<div class="chart-subtitle">Random Forest vs Gradient Boosting</div>',
                    unsafe_allow_html=True)
        fig_models = go.Figure()
        fig_models.add_trace(go.Bar(
            name='Random Forest',
            x=['RMSE (kWh/m²)', 'R²'],
            y=[30.31, 0.877],
            marker_color='rgba(122,179,212,0.6)',
            marker_line_width=0,
        ))
        fig_models.add_trace(go.Bar(
            name='GBT (meilleur)',
            x=['RMSE (kWh/m²)', 'R²'],
            y=[28.82, 0.889],
            marker_color='#00a3e0',
            marker_line_width=0,
        ))
        fig_models.update_layout(
            **PLOTLY_LAYOUT, height=300, barmode='group',
            legend=dict(font=dict(color='#7ab3d4'))
        )
        st.plotly_chart(fig_models, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    with col2:
        st.markdown('<div class="chart-card">', unsafe_allow_html=True)
        st.markdown('<div class="chart-title">Importance des variables</div>',
                    unsafe_allow_html=True)
        st.markdown('<div class="chart-subtitle">Contribution au modèle GBT</div>',
                    unsafe_allow_html=True)
        features = pd.DataFrame({
            'feature': [
                'etiquette_dpe', 'ubat_w_par_m2_k', 'deperditions_enveloppe',
                'periode_construction', 'qualite_isolation_murs',
                'zone_climatique', 'type_energie_chauffage'
            ],
            'importance': [34, 22, 16, 11, 8, 5, 4]
        }).sort_values('importance')
        fig_fi = px.bar(
            features, x='importance', y='feature', orientation='h',
            color='importance',
            color_continuous_scale=['#0066a4', '#00a3e0'],
        )
        fig_fi.update_layout(**PLOTLY_LAYOUT, coloraxis_showscale=False, height=300)
        st.plotly_chart(fig_fi, use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="chart-card">', unsafe_allow_html=True)
    st.markdown('<div class="chart-title">Pipeline complet du projet</div>',
                unsafe_allow_html=True)
    st.markdown("""
    <div style="display:flex;align-items:center;justify-content:center;gap:8px;
                padding:20px 0;flex-wrap:wrap;">
        <div style="text-align:center;padding:16px 20px;background:rgba(0,163,224,0.1);
                    border:1px solid rgba(0,163,224,0.3);border-radius:8px;min-width:120px;">
            <div style="font-size:10px;color:#7ab3d4;text-transform:uppercase;
                        letter-spacing:1px;margin-bottom:6px;">Sources</div>
            <div style="font-size:14px;color:#e8f4fd;font-weight:600;">data.gouv.fr</div>
            <div style="font-size:11px;color:#7ab3d4;margin-top:4px;">DPE + Conso + Adresses</div>
        </div>
        <div style="color:#00a3e0;font-size:22px;">→</div>
        <div style="text-align:center;padding:16px 20px;background:rgba(255,140,0,0.1);
                    border:1px solid rgba(255,140,0,0.3);border-radius:8px;min-width:120px;">
            <div style="font-size:10px;color:#ff8c00;text-transform:uppercase;
                        letter-spacing:1px;margin-bottom:6px;">Ingestion</div>
            <div style="font-size:14px;color:#e8f4fd;font-weight:600;">Kafka</div>
            <div style="font-size:11px;color:#7ab3d4;margin-top:4px;">Producer / Consumer</div>
        </div>
        <div style="color:#00a3e0;font-size:22px;">→</div>
        <div style="text-align:center;padding:16px 20px;background:rgba(0,163,224,0.1);
                    border:1px solid rgba(0,163,224,0.3);border-radius:8px;min-width:120px;">
            <div style="font-size:10px;color:#7ab3d4;text-transform:uppercase;
                        letter-spacing:1px;margin-bottom:6px;">Bronze</div>
            <div style="font-size:14px;color:#e8f4fd;font-weight:600;">MinIO</div>
            <div style="font-size:11px;color:#7ab3d4;margin-top:4px;">Données brutes</div>
        </div>
        <div style="color:#00a3e0;font-size:22px;">→</div>
        <div style="text-align:center;padding:16px 20px;background:rgba(0,200,100,0.1);
                    border:1px solid rgba(0,200,100,0.3);border-radius:8px;min-width:120px;">
            <div style="font-size:10px;color:#00c864;text-transform:uppercase;
                        letter-spacing:1px;margin-bottom:6px;">Silver</div>
            <div style="font-size:14px;color:#e8f4fd;font-weight:600;">Spark</div>
            <div style="font-size:11px;color:#7ab3d4;margin-top:4px;">Nettoyage Parquet</div>
        </div>
        <div style="color:#00a3e0;font-size:22px;">→</div>
        <div style="text-align:center;padding:16px 20px;background:rgba(255,200,0,0.1);
                    border:1px solid rgba(255,200,0,0.3);border-radius:8px;min-width:120px;">
            <div style="font-size:10px;color:#ffc800;text-transform:uppercase;
                        letter-spacing:1px;margin-bottom:6px;">Gold</div>
            <div style="font-size:14px;color:#e8f4fd;font-weight:600;">GBT ML</div>
            <div style="font-size:11px;color:#7ab3d4;margin-top:4px;">R² = 0.889</div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)