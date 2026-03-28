import streamlit as st
import pandas as pd

st.title("📊 Dashboard DPE")

# charger données
df = pd.read_csv("data_lake/dashboard/dpe")

st.write("Aperçu des données")
st.dataframe(df)

# graphique
st.subheader("Consommation moyenne par DPE")
st.bar_chart(df.set_index("etiquette_dpe"))