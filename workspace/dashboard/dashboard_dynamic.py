import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import time
import random

st.set_page_config(page_title="Dashboard Ventes Silver", layout="wide")
st.title("Dashboard Ventes Silver")

# --- Fonctions pour simuler les données ---
def generate_bronze_data():
    data = [
        ("FR", "Retail", random.randint(1, 50), random.randint(50, 500)),
        ("FR", "Online", random.randint(1, 30), random.randint(20, 300)),
        ("US", "Retail", random.randint(1, 70), random.randint(100, 700)),
        ("US", "Online", random.randint(1, 50), random.randint(80, 600)),
    ]
    df = pd.DataFrame(data, columns=["pays", "segment", "quantite", "total"])
    return df

def aggregate_silver(df_bronze):
    df_silver = df_bronze.groupby(["pays", "segment"], as_index=False).agg(
        total_quantite=pd.NamedAgg(column="quantite", aggfunc="sum"),
        ca_total=pd.NamedAgg(column="total", aggfunc="sum")
    )
    return df_silver

# --- Zone de mise à jour dynamique ---
placeholder = st.empty()
historical_data = pd.DataFrame(columns=["rafraichissement", "pays", "segment", "ca_total", "total_quantite"])

for i in range(50):
    df_bronze = generate_bronze_data()
    df_silver = aggregate_silver(df_bronze)
    df_silver["rafraichissement"] = i + 1
    
    # Stocker l'historique pour line chart
    historical_data = pd.concat([historical_data, df_silver], ignore_index=True)
    
    with placeholder.container():
        st.subheader(f"Mise à jour du dashboard (rafraîchissement {i+1})")
        st.dataframe(df_silver)
        
        # --- Bar chart CA ---
        fig_ca = px.bar(
            df_silver,
            x="pays",
            y="ca_total",
            color="segment",
            text="ca_total",
            title="Chiffre d'affaires par pays et segment"
        )
        st.plotly_chart(fig_ca, use_container_width=True)
        
        # --- Bar chart Quantité ---
        fig_qty = px.bar(
            df_silver,
            x="pays",
            y="total_quantite",
            color="segment",
            text="total_quantite",
            title="Quantité vendue par pays et segment"
        )
        st.plotly_chart(fig_qty, use_container_width=True)
        
        # --- Pie chart CA par segment ---
        fig_pie = px.pie(
            df_silver,
            names="segment",
            values="ca_total",
            title="Répartition du CA par segment"
        )
        st.plotly_chart(fig_pie, use_container_width=True)
        
        # --- Line chart évolution du CA ---
        fig_line = px.line(
            historical_data,
            x="rafraichissement",
            y="ca_total",
            color="segment",
            line_group="pays",
            markers=True,
            title="Évolution du CA au fil du temps"
        )
        st.plotly_chart(fig_line, use_container_width=True)
        
        # --- Heatmap CA par pays et segment ---
        heat_data = df_silver.pivot(index="pays", columns="segment", values="ca_total").fillna(0)
        fig_heat = go.Figure(data=go.Heatmap(
            z=heat_data.values,
            x=heat_data.columns,
            y=heat_data.index,
            colorscale="Viridis"
        ))
        fig_heat.update_layout(title="Heatmap CA par pays et segment")
        st.plotly_chart(fig_heat, use_container_width=True)
    
    time.sleep(5)
