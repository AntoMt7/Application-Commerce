import streamlit as st
import pydeck as pdk
import io
import snowflake.connector
from snowflake.snowpark.session import Session
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go 

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
    query = "UPDATE appli_commerce.public.commerce SET COMMENTAIRES = ? WHERE NOM = ?"
    session.sql(query, [commentaire, nom]).collect()

# Fonctions pour récupérer les données
def get_regions_from_secteurs(secteurs):
    placeholders = ",".join(["?"] * len(secteurs))
    query = f"SELECT DISTINCT REGION FROM appli_commerce.public.commerce WHERE SECTEUR_D_ACTIVITE IN ({placeholders}) ORDER BY REGION"
    result = session.sql(query, secteurs).collect()
    return [row["REGION"] for row in result]

def get_size_from_secteurs(secteurs):
    placeholders = ",".join(["?"] * len(secteurs))
    query = f"SELECT DISTINCT SIZE FROM appli_commerce.public.commerce WHERE SECTEUR_D_ACTIVITE IN ({placeholders}) ORDER BY TRY_CAST(SPLIT_PART(SIZE, '-', 1) AS INTEGER)"
    result = session.sql(query, secteurs).collect()
    return [row["SIZE"] for row in result]

def get_departement(region_choisie):
    query = "SELECT DISTINCT DEPARTEMENT FROM appli_commerce.public.commerce WHERE REGION = ? ORDER BY DEPARTEMENT ASC"
    result = session.sql(query, [region_choisie]).collect()
    return [row["DEPARTEMENT"] for row in result]

def get_all_secteurs():
    query = "SELECT DISTINCT SECTEUR_D_ACTIVITE FROM appli_commerce.public.commerce ORDER BY SECTEUR_D_ACTIVITE"
    result = session.sql(query).collect()
    return [row["SECTEUR_D_ACTIVITE"] for row in result]

def get_industries_from_secteurs(secteurs):
    placeholders = ",".join(["?"] * len(secteurs))
    query = f"SELECT DISTINCT INDUSTRIE FROM appli_commerce.public.commerce WHERE SECTEUR_D_ACTIVITE IN ({placeholders}) ORDER BY INDUSTRIE"
    result = session.sql(query, secteurs).collect()
    return [row["INDUSTRIE"] for row in result]


def get_entreprises(
    secteurs=None,
    industries=None,
    regions=None,
    departements=None,
    tailles=None
):
    query = """
        SELECT NOM, CREATION, VILLE, SITE_INTERNET, LINKEDIN_URL, SIZE, INDUSTRIE,
               COMMENTAIRES, LON, LAT
        FROM appli_commerce.public.commerce
        WHERE 1=1
    """
    params = []

    # Ajout dynamique des clauses WHERE
    if secteurs:
        placeholders = ",".join(["?"] * len(secteurs))
        query += f" AND SECTEUR_D_ACTIVITE IN ({placeholders})"
        params.extend(secteurs)

    if industries and "Aucune industrie" not in industries:
        placeholders = ",".join(["?"] * len(industries))
        query += f" AND INDUSTRIE IN ({placeholders})"
        params.extend(industries)

    if regions:
        placeholders = ",".join(["?"] * len(regions))
        query += f" AND REGION IN ({placeholders})"
        params.extend(regions)

    if departements:
        placeholders = ",".join(["?"] * len(departements))
        query += f" AND DEPARTEMENT IN ({placeholders})"
        params.extend(departements)

    if tailles:
        placeholders = ",".join(["?"] * len(tailles))
        query += f" AND SIZE IN ({placeholders})"
        params.extend(tailles)

    # Exécuter la requête
    result = session.sql(query, params).to_pandas()

    # Supprimer les entreprises sans coordonnées
    result = result.dropna(subset=["LAT", "LON"])

    # Grouper les entreprises par ville
    grouped_data = result.groupby(["VILLE", "LAT", "LON"]).apply(
        lambda x: ", ".join(f"{row['NOM']} ({row['SIZE']} employés)" for _, row in x.iterrows())
    ).reset_index(name="ENTREPRISES")

    # Retourner les données
    return result.drop(columns=["LON", "LAT"]), grouped_data

# Fonction pour récupérer les années disponibles
def get_years(session):
    query = "SELECT DISTINCT CREATION FROM appli_commerce.public.commerce ORDER BY CREATION ASC"
    result = session.sql(query).collect()
    return [row["CREATION"] for row in result]

# Fonction pour générer un CSV à partir du DataFrame
def to_csv(df):
    csv = df.to_csv(index=False)  # Convertir le DataFrame en CSV sans index
    return csv

# Interface utilisateur
with st.sidebar:
    st.title("Prospection commerciale")

    # Connexion
    session = get_snowflake_session()

    # 1. Secteur d’activité
    secteurs = get_all_secteurs()  # Une nouvelle fonction à créer (voir plus bas)
    secteur_choisi = st.multiselect("Sélectionner un/des secteur(s) d'activité(s)", secteurs)

    # 2. Industrie
    industrie_choisie = []
    if secteur_choisi:
        industries = get_industries_from_secteurs(secteur_choisi)
        industries = ["Aucune industrie"] + industries
        industrie_choisie = st.multiselect("Sélectionner une ou plusieurs industrie(s)", industries)

    # 3. Région
    region_choisie = []
    if secteur_choisi:
        regions = get_regions_from_secteurs(secteur_choisi)
        region_choisie = st.multiselect("Sélectionner une ou plusieurs région(s)", regions)

    # 4. Département
    departement_choisie = []
    if region_choisie:
        departements = get_departement(region_choisie)
        departement_choisie = st.multiselect("Sélectionner un ou plusieurs département(s)", departements)

    # 5. Taille
    size_choisies = []
    if secteur_choisi:
        sizes = get_size_from_secteurs(secteur_choisi)
        size_choisies = st.multiselect("Sélectionner une ou plusieurs tailles d'entreprise", sizes)

# Main content area
# Création des onglets
#tab1, tab2 = st.tabs(["Carte & Données", "Analyses & Graphiques"])
#with tab1: 
if secteur_choisi:
    entreprises, map_data = get_entreprises(region_choisie, departement_choisie, size_choisies, industrie_choisie, secteur_choisi)

    if not entreprises.empty:
        st.write(
            f"Tableau des entreprises dans la région '{region_choisie}', département '{departement_choisie}', "
            f"tailles {size_choisies}, secteur d'activité '{secteur_choisi}' :"
        )

        # Afficher le tableau avec les nouvelles colonnes, et permettre l'édition de la colonne COMMENTAIRES
        edited_df = st.data_editor(
            entreprises[["NOM", "CREATION", "VILLE", "SIZE", "SITE_INTERNET", "LINKEDIN_URL", "COMMENTAIRES"]],
            use_container_width=True,
            height=600,
            column_config={
                "COMMENTAIRES": st.column_config.TextColumn("Commentaires")
            }
        )

        # Sauvegarde des commentaires modifiés
        if not entreprises["COMMENTAIRES"].equals(edited_df["COMMENTAIRES"]):
            for index, row in edited_df.iterrows():
                if row["COMMENTAIRES"] != entreprises.at[index, "COMMENTAIRES"]:
                    save_commentaire(row["NOM"], row["COMMENTAIRES"])
            st.success("Les commentaires ont été mis à jour.")

        # Ajouter le bouton de téléchargement CSV avec toutes les colonnes
        csv_data = to_csv(entreprises)
        st.download_button(
            label="Télécharger en CSV",
            data=csv_data,
            file_name="entreprises.csv",
            mime="text/csv"
        )

        # Vérifier si la carte peut être affichée
        if not map_data.empty:
            st.subheader("Localisation des Entreprises")
            layer = pdk.Layer(
                "ScatterplotLayer",
                data=map_data,
                get_position=["LON", "LAT"],
                get_color=[255, 0, 0, 140],
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
                map_style="mapbox://styles/mapbox/light-v9",
                tooltip={"html": "<b>Ville:</b> {VILLE}<br><b>Entreprises:</b> {ENTREPRISES}"}
            )

            st.pydeck_chart(deck)


            
#with tab2:
 #   st.header("Analyses et Statistiques")

  #  if "entreprises" in locals() and not entreprises.empty:
   #     st.metric(label="Année moyenne de création", value=round(entreprises['CREATION'].mean(), 2))
    #    col1, col2 = st.columns(2)

        # Distribution par taille d'entreprise
     #   with col1:
      #      st.subheader("Distribution des entreprises par taille")
       #     size_distribution = entreprises['SIZE'].value_counts()
        #    fig_size = px.pie(
         #       values=size_distribution.values,
          #      names=size_distribution.index,
           #     title="Répartition des entreprises par nombre d'employés"
            #)
            #st.plotly_chart(fig_size)

        # Répartition par ville
        #with col2:
         #   st.subheader("Répartition des entreprises par ville")
          #  city_counts = entreprises['VILLE'].value_counts().head(10)
           # fig_city = px.bar(
            #    x=city_counts.index,
             #   y=city_counts.values,
              #  labels={'x': 'Ville', 'y': "Nombre d'entreprises"},
               # title="Top 10 des villes par nombre d'entreprises"
            #)
            #fig_city.update_xaxes(tickangle=45)
            #st.plotly_chart(fig_city)

   # else:
    #    st.warning("Aucune entreprise ne correspond aux critères sélectionnés.")
