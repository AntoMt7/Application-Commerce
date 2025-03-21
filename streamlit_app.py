import streamlit as st
import pydeck as pdk
import io
import snowflake.connector
from snowflake.snowpark.session import Session
from snowflake.snowpark.context import get_active_session
import pandas as pd

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

# Fonction pour rajouter des commentaires
def save_commentaire(nom, commentaire):
    """Met à jour le commentaire dans la base de données Snowflake."""
    query = "UPDATE geo_com.public.test SET COMMENTAIRES = ? WHERE NOM = ?"
    session.sql(query, [commentaire, nom]).collect()

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
    SELECT DISTINCT SECTEUR_D_ACTIVITE 
    FROM geo_com.public.test 
    WHERE REGION = ? AND DEPARTEMENT = ? AND SIZE IN ({','.join(['?'] * len(size_choisies))})
    ORDER BY SECTEUR_D_ACTIVITE ASC
    """
    result = session.sql(query, [region_choisie, departement_choisie] + size_choisies).collect()
    return [row["SECTEUR_D_ACTIVITE"] for row in result]

def get_entreprises(region_choisie, departement_choisie, size_choisies, industrie_choisie):
    query = f"""
    SELECT NOM, CREATION, VILLE, SITE_INTERNET, LINKEDIN_URL, SIZE, INDUSTRIE, COMMENTAIRES, LON, LAT
    FROM geo_com.public.test
    WHERE REGION = ? AND DEPARTEMENT = ? AND SIZE IN ({','.join(['?'] * len(size_choisies))}) AND SECTEUR_D_ACTIVITE = ?
    """
    result = session.sql(query, [region_choisie, departement_choisie] + size_choisies + [industrie_choisie]).to_pandas()

    # Supprime les entreprises sans coordonnées
    result = result.dropna(subset=["LAT", "LON"])

    # Concaténer les noms des entreprises par ville
    grouped_data = result.groupby(["VILLE", "LAT", "LON"]).apply(
        lambda x: ", ".join(f"{row['NOM']} ({row['SIZE']} employés)" for _, row in x.iterrows())
    ).reset_index(name="ENTREPRISES")

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

# Fonction pour afficher et sauvegarder les commentaires
def afficher_et_sauvegarder_commentaires(entreprises):
    for index, row in entreprises.iterrows():
        key = row["NOM"]
        
        # Récupération du commentaire existant, s'il y en a
        commentaire_actuel = row["COMMENTAIRES"] if pd.notna(row["COMMENTAIRES"]) else ""
        
        # Champ pour entrer un commentaire (texte multi-ligne)
        new_comment = st.text_area(f"Commentaire pour {key}:",
                                   value=commentaire_actuel, 
                                   key=f"comment_{index}")
        
        # Bouton pour enregistrer le commentaire
        if st.button(f"Enregistrer pour {key}", key=f"save_{index}"):
            if new_comment != commentaire_actuel:  # Vérifie si le commentaire a changé
                save_commentaire(key, new_comment)
                st.success(f"Commentaire mis à jour pour {key}!")
            else:
                st.info(f"Aucun changement pour {key}.")

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
    industrie_choisie = st.selectbox("Sélectionner un secteur d'activité", existing_industrie)

# Affichage des résultats si tous les critères sont remplis
if industrie_choisie:
    entreprises, map_data = get_entreprises(region_choisie, departement_choisie, size_choisies, industrie_choisie)

    if not entreprises.empty:
        st.write(f"Tableau des entreprises dans la région '{region_choisie}', département '{departement_choisie}', tailles {size_choisies}, SECTEUR_D_ACTIVITE '{industrie_choisie}' :")
        
        # Afficher le tableau avec les nouvelles colonnes
        st.table(entreprises[["NOM", "CREATION", "VILLE", "SITE_INTERNET", "LINKEDIN_URL", "SIZE", "INDUSTRIE", "COMMENTAIRES"]])
    
        # Ajouter le bouton de téléchargement CSV avec toutes les colonnes
        csv_data = to_csv(entreprises)
        st.download_button(
            label="Télécharger en CSV",
            data=csv_data,
            file_name="entreprises.csv",
            mime="text/csv"
        )

        # Afficher les champs de commentaire et les enregistrer
        afficher_et_sauvegarder_commentaires(entreprises)

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
