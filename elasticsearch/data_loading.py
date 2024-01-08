"""
Project: AD4IDS - Anomaly Detection for Intrusion Detection Systems
Subproject: 1 - Flow Classification
Stage: 1 - Data loading from XML files
Authors: MONNIER Killian & BAKKARI Ikrame
Date: 10/2023
"""

import os
import glob
from lxml import etree
import pandas as pd


# Définir une fonction pour analyser un fichier XML et le convertir en liste de dictionnaires
def parse_xml_file(xml_file):
    flow_data = []
    tree = etree.parse(xml_file)
    root = tree.getroot()

    for flow_element in root:
        flow_dict = {}
        for child in flow_element:
            flow_dict[child.tag] = child.text
        flow_data.append(flow_dict)

    return flow_data


# Spécifiez le répertoire contenant les fichiers XML
xml_files_dir = "data/TRAIN_ENSIBS"

# Obtenez une liste de tous les fichiers XML du répertoire
xml_files = glob.glob(os.path.join(xml_files_dir, "*.xml"))

# Initialisez une liste vide pour stocker toutes les données de flux
all_flow_data = []

# Parcourez chaque fichier XML et analysez-le
for xml_file in xml_files:
    flow_data = parse_xml_file(xml_file)

    origin = os.path.basename(xml_file)  # Obtenez le nom du fichier d'origine
    # Ajoutez le champ "origin" à chaque élément de flow_data
    for flow in flow_data:
        flow["origin"] = origin

    all_flow_data.extend(flow_data)

# Convertir en DataFrame
flow_df = pd.DataFrame(all_flow_data)

# Sauvegarder en fichier pickle
flow_df.to_pickle("data/flow_df.pkl")

# Désormais, all_flow_data contient une liste de dictionnaires, où chaque dictionnaire correspond à un seul flux
# Vous pouvez accéder et manipuler les données selon vos besoins

print("Longueur: ", len(flow_df))
print(flow_df.head())
