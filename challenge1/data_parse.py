"""
Project: AD4IDS - Anomaly Detection for Intrusion Detection Systems
Subproject: Challenge 1 - HTTPWeb & SSH
Stage: 1 - Data parsing & Preprocessing
Authors: MONNIER Killian & BAKKARI Ikrame
Date: 01/2024
"""

import xml.etree.ElementTree as ET
import pandas as pd
import ipaddress
import pickle
import sys
import os

# Ajouter le dossier parent au sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from preprocess_df import preprocess_data

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


def parse_xml_to_dataframe(file_name):
    print("Parsing", file_name, "XML file...")

    # Charger et parser le fichier XML
    tree = ET.parse(file_name)
    root = tree.getroot()

    # Initialiser une liste pour stocker les données
    data = []

    # Boucler sur chaque élément dans la balise <test>
    for item in root.findall(".//item"):
        # Extraire les informations de chaque sous-élément
        record = {}
        for child in item:
            record[
                child.tag
            ] = child.text  # Créer une entrée pour chaque balise et sa valeur

        # Ajouter le dictionnaire au tableau de données
        data.append(record)

    # Convertir le tableau de données en DataFrame Pandas
    df = pd.DataFrame(data)
    print("Done parsing XML file in dataframe.")
    print(df.head())

    # Ici, ajoutez tout prétraitement nécessaire
    df_preprocessed = preprocess_data(df)
    print("Done preprocessing data in dataframe.")
    print(df_preprocessed.head())

    return df_preprocessed


# Parsing des fichiers XML
df_test = parse_xml_to_dataframe("challenge1/data/benchmark_HTTPWeb_test.xml")
# df_test = parse_xml_to_dataframe("challenge1/data/benchmark_SSH_test.xml")

# Charger le jeu de données complet
with open("data/df_preprocessed.pkl", "rb") as file:
    df_complet = pd.read_pickle(file)

# Charger le jeu de données incomplet
df_incomplet = df_test

# Obtenir la liste des colonnes manquantes
missing_columns = set(df_complet.columns) - set(df_incomplet.columns)
print(missing_columns)

# Ajouter les colonnes manquantes au DataFrame incomplet
for column in missing_columns:
    df_incomplet[column] = 0  # Remplir de zéros

# Réorganiser les colonnes pour qu'elles correspondent à celles du jeu de données complet
df_test = df_incomplet[df_complet.columns]

# Sauvegarder le DataFrame avec les données manquantes
df_test = df_test.drop(columns=["appName", "origin"], errors="ignore")
df_test.fillna(
    0, inplace=True
)  # Remplacez toutes les valeurs NaN par 0 dans l'ensemble du DataFrame

# Sauvegarder le DataFrame avec les features manquantes
with open("challenge1/data/df_HTTPWeb.pkl", "wb") as file:
    # with open("challenge1/data/df_SSH.pkl", "wb") as file:
    pickle.dump(df_test, file)
