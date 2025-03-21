import streamlit as st
import pydeck as pdk
import io
import snowflake.connector
from snowflake.snowpark.session import Session
from snowflake.snowpark.context import get_active_session

# Connexion à Snowflake
def get_snowflake_session():
    connection_parameters = {
        "user": st.secrets["snowflake"]["user"],
        "password": st.secrets["snowflake"]["password"],
        "account": st.secrets["snowflake"]["account"],
        "warehouse": st.secrets["snowflake"]["warehouse"],
        "database": st.secrets["snowflake"]["database"],
        "schema": st.secrets["snowflake"]["schema"]
    }
    return Session.builder.configs(connection_parameters).create()
 
# Fonctions pour récupérer les données
def get_region():
    query = "SELECT DISTINCT REGION FROM geo_com.public.test ORDER BY REGION ASC"
    result = session.sql(query).collect()
    return [row["REGION"] for row in result]

def get_size():
    query = "SELECT DISTINCT SIZE FROM geo_com.public.test ORDER BY SIZE ASC"
    result = session.sql(query).collect()
    return [row["SIZE"] for row in result]

def get_departement(region_choisie):
    query = "SELECT DISTINCT DEPARTEMENT FROM geo_com.public.test WHERE REGION = ? ORDER BY DEPARTEMENT ASC"
    result = session.sql(query, [region_choisie]).collect()
    return [row["DEPARTEMENT"] for row in result]

def get_industrie(region_choisie, size_choisies, departement_choisie):
    query = f"""
    SELECT DISTINCT Secteur_d_activite 
    FROM geo_com.public.test 
    WHERE REGION = ? AND DEPARTEMENT = ? AND SIZE IN ({','.join(['?'] * len(size_choisies))})
    ORDER BY Secteur_d_activite ASC
    """
    result = session.sql(query, [region_choisie, departement_choisie] + size_choisies).collect()
    return [row["Secteur_d_activite"] for row in result]

def get_entreprises(region_choisie, departement_choisie, size_choisies, industrie_choisie):
    query = f"""
    SELECT NOM, CREATION, VILLE, SITE_INTERNET, LINKEDIN_URL, LON, LAT
    FROM geo_com.public.test
    WHERE REGION = ? AND DEPARTEMENT = ? AND SIZE IN ({','.join(['?'] * len(size_choisies))}) AND Secteur_d_activite  = ?
    """
    result = session.sql(query, [region_choisie, departement_choisie] + size_choisies + [industrie_choisie]).to_pandas()
    
    # Supprime les entreprises sans coordonnées
    result = result.dropna(subset=["LAT", "LON"])

    # Concaténer les noms des entreprises par ville
    grouped_data = result.groupby(["VILLE", "LAT", "LON"])["NOM"].apply(lambda x: ", ".join(x)).reset_index()
    grouped_data.rename(columns={"NOM": "ENTREPRISES"}, inplace=True)

    return result.drop(columns=["LON", "LAT"]), grouped_data

# Fonction pour récupérer les années disponibles
def get_years():
    query = "SELECT DISTINCT CREATION FROM geo_com.public.test ORDER BY CREATION ASC"
    result = session.sql(query).collect()
    return [row["CREATION"] for row in result]

# Fonction pour générer un CSV à partir du DataFrame
def to_csv(df):
    csv = df.to_csv(index=False)  # Convertir le DataFrame en CSV sans index
    return csv
    
# Interface utilisateur
st.title("Application commerciale")
session = get_snowflake_session()
# Sélection de la région
existing_regions = get_region()
region_choisie = st.selectbox("Sélectionner une région", existing_regions)

# Mise à jour dynamique des départements en fonction de la région
departement_choisie = None
if region_choisie:
    departements = get_departement(region_choisie)
    departement_choisie = st.selectbox("Sélectionner le département souhaité", departements)

# Sélection multiple de la taille de l'entreprise
size_choisies = []
if departement_choisie:
    sizes = get_size()
    size_choisies = st.multiselect("Sélectionner une ou plusieurs tailles d'entreprise", sizes)

# Sélection de l'industrie avec filtrage dynamique
industrie_choisie = None
if size_choisies:
    existing_industrie = get_industrie(region_choisie, size_choisies, departement_choisie)
    industrie_choisie = st.selectbox("Sélectionner une industrie", existing_industrie)

# Affichage des résultats si tous les critères sont remplis
if industrie_choisie:
    entreprises, map_data = get_entreprises(region_choisie, departement_choisie, size_choisies, industrie_choisie)

    if not entreprises.empty:
        st.write(f"Tableau des entreprises dans la région '{region_choisie}', département '{departement_choisie}', tailles {size_choisies}, industrie '{industrie_choisie}' :")
        st.table(entreprises)  # On affiche les entreprises sans LON/LAT
        # Ajouter un bouton pour télécharger les données du tableau
        csv_data = to_csv(entreprises)  # Convertir les données du tableau en CSV
        st.download_button(
            label="Télécharger en CSV",
            data=csv_data,
            file_name="entreprises.csv",
            mime="text/csv"
        )
        # Vérifier si la carte peut être affichée
        if not map_data.empty:
            layer = pdk.Layer(
                "ScatterplotLayer",
                data=map_data,
                get_position=["LON", "LAT"],
                get_color=[255, 0, 0, 140],  # Rouge semi-transparent
                get_radius=500,
                pickable=True,
            )

            view_state = pdk.ViewState(
                latitude=map_data["LAT"].mean(),
                longitude=map_data["LON"].mean(),
                zoom=10
            )

            deck = pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                map_style="mapbox://styles/mapbox/light-v9",  # Fond de carte clair
                tooltip={"html": "<b>Ville:</b> {VILLE}<br><b>Entreprises:</b> {ENTREPRISES}"}
            )

            st.pydeck_chart(deck)
        else:
            st.write("Aucune donnée de localisation disponible pour affichage sur la carte.")
    else:
        st.write("Aucune entreprise ne correspond aux critères sélectionnés.")
