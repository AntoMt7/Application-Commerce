import streamlit as st
import pydeck as pdk
import io
import snowflake.connector
from snowflake.snowpark.session import Session
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

def get_industries_for_secteur(region_choisie, size_choisies, departement_choisie, secteur_choisi):
    query = f"""
    SELECT DISTINCT INDUSTRIE 
    FROM geo_com.public.test 
    WHERE REGION = ? AND DEPARTEMENT = ? AND SIZE IN ({','.join(['?'] * len(size_choisies))}) AND SECTEUR_D_ACTIVITE = ?
    ORDER BY INDUSTRIE ASC
    """
    result = session.sql(query, [region_choisie, departement_choisie] + size_choisies + [secteur_choisi]).collect()
    return [row["INDUSTRIE"] for row in result]

def get_entreprises(region_choisie, departement_choisie, size_choisies, industrie_choisie=None, secteur_choisi=None):
    query = f"""
    SELECT NOM, CREATION, VILLE, SITE_INTERNET, LINKEDIN_URL, SIZE, INDUSTRIE, COMMENTAIRES, LON, LAT
    FROM geo_com.public.test
    WHERE REGION = ? AND DEPARTEMENT = ? AND SIZE IN ({','.join(['?'] * len(size_choisies))})
    """
    
    params = [region_choisie, departement_choisie] + size_choisies
    
    # Ajouter le filtre sur le secteur d'activité si sélectionné
    if secteur_choisi:
        query += " AND SECTEUR_D_ACTIVITE = ?"
        params.append(secteur_choisi)
    
    # Ajouter le filtre sur l'industrie si sélectionnée
    if industrie_choisie and industrie_choisie != "Aucune industrie":
        query += " AND INDUSTRIE = ?"
        params.append(industrie_choisie)
    
    result = session.sql(query, params).to_pandas()

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

# Sélection du secteur d'activité avec filtrage dynamique
secteur_choisi = None
if size_choisies:
    existing_secteurs = get_industrie(region_choisie, size_choisies, departement_choisie)
    secteur_choisi = st.selectbox("Sélectionner un secteur d'activité", existing_secteurs)

# Sélection dynamique de l'industrie en fonction du secteur choisi
industrie_choisie = None
if secteur_choisi:
    existing_industries = get_industries_for_secteur(region_choisie, size_choisies, departement_choisie, secteur_choisi)
    # Ajouter une option pour "Aucune industrie" pour réinitialiser le filtre
    existing_industries = ["Aucune industrie"] + existing_industries
    industrie_choisie = st.selectbox("Sélectionner une industrie", existing_industries)

# Affichage des résultats si tous les critères sont remplis
if secteur_choisi:
    entreprises, map_data = get_entreprises(region_choisie, departement_choisie, size_choisies, industrie_choisie, secteur_choisi)

    if not entreprises.empty:
        st.write(f"Tableau des entreprises dans la région '{region_choisie}', département '{departement_choisie}', tailles {size_choisies}, secteur d'activité '{secteur_choisi}' :")
        
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

        # Sélectionner une entreprise à partir du tableau pour ajouter un commentaire
        entreprise_choisie = st.selectbox("Sélectionner une entreprise pour ajouter un commentaire", entreprises["NOM"].tolist())

        # Récupération du commentaire existant, s'il y en a
        entreprise_selected = entreprises[entreprises["NOM"] == entreprise_choisie].iloc[0]
        commentaire_actuel = entreprise_selected["COMMENTAIRES"] if pd.notna(entreprise_selected["COMMENTAIRES"]) else ""
        
        # Affichage de la zone de texte pour ajouter/modifier un commentaire
        new_comment = st.text_area(f"Commentaire pour {entreprise_choisie}:",
                                   value=commentaire_actuel, 
                                   key=f"comment_{entreprise_choisie}")
        
        # Bouton pour enregistrer le commentaire
        if st.button(f"Enregistrer pour {entreprise_choisie}") :
            if new_comment != commentaire_actuel:  # Vérifie si le commentaire a changé
                save_commentaire(entreprise_choisie, new_comment)
                st.success(f"Commentaire mis à jour pour {entreprise_choisie}!")
            else:
                st.info(f"Aucun changement pour {entreprise_choisie}.")

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
