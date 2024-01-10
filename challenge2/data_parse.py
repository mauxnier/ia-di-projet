"""
Project: AD4IDS - Anomaly Detection for Intrusion Detection Systems
Subproject: Challenge 2
Stage: 1 - Data parsing
Authors: MONNIER Killian & BAKKARI Ikrame
Date: 01/2024

@deprecated - On utilise challenge2.ipynb à la place. Utilisation de csv au lien de xml.
"""

import xml.etree.ElementTree as ET
import xml.sax
import os
import pandas as pd
import pickle
from FlowHandler import FlowHandler

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

xml_file = "challenge2/data/traffic_os_TRAIN.xml"
# xml_file = "challenge2/data/traffic_os_TEST.xml"
xml_file_size = os.path.getsize(xml_file)
df_file = "challenge2/data/df_traffic_train.pkl"
# df_file = "challenge2/data/df_traffic_test.pkl"


def parse_xml_to_dataframe(file_name):
    print("Parsing", file_name, "XML file...")

    # Charger et parser le fichier XML
    tree = ET.parse(file_name)
    root = tree.getroot()

    # Initialiser une liste pour stocker les données
    data = []

    # Boucler sur chaque élément dans la balise <test>
    for item in root.findall(".//FFlow"):
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

    return df


# Créer un analyseur SAX
parser = xml.sax.make_parser()

# Désactiver les espaces de noms
parser.setFeature(xml.sax.handler.feature_namespaces, 0)

# Override the default ContextHandler
Handler = FlowHandler(xml_file_size)
parser.setContentHandler(Handler)

# Lire le fichier XML
parser.parse(xml_file)

# Convertir la liste de dictionnaires en DataFrame
df_data = pd.DataFrame(Handler.flows)
print(df_data.head())

# Sauvegarder le DataFrame avec les features manquantes
# with open(df_file, "wb") as file:
#     pickle.dump(df_data, file)
# pd.to_pickle(df_data, df_file)

df_data.to_pickle(df_file)

# part_size = 500000  # Nombre de lignes par partie
# num_parts = len(df_data) // part_size + (1 if len(df_data) % part_size else 0)

# for part in range(num_parts):
#     start_row = part * part_size
#     end_row = start_row + part_size
#     part_df = df_data.iloc[start_row:end_row]
#     part_df.to_pickle(df_file + f"_{part}.pkl")
