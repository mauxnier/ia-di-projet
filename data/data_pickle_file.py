import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch

# Initialisation de la connexion Elasticsearch
es = Elasticsearch(hosts=["http://localhost:9200"])

# Définir le nom de l'index
index_name = "flow_data_enc_6"

# Apply transport options to the Elasticsearch object
es.options(request_timeout=60, max_retries=5, retry_on_timeout=True)

# Préparer la requête
query = {"match_all": {}}

# Initialiser la recherche de récupération des données depuis Elasticsearch
result = es.search(index=index_name, query=query, scroll="2m", size=10000)

# Initialiser un DataFrame vide pour stocker toutes les données
all_data = pd.DataFrame()

# Récupérer les données par lots
while len(result["hits"]["hits"]) > 0:
    # Créer un DataFrame pour le lot actuel
    df = pd.DataFrame(hit["_source"] for hit in result["hits"]["hits"])

    # Concaténer le lot actuel au DataFrame global
    all_data = pd.concat([all_data, df], ignore_index=True)

    # Utiliser l'ID de scroll pour récupérer le prochain lot
    result = es.scroll(scroll_id=result["_scroll_id"], scroll="2m")

# Enregistrer le DataFrame global dans un fichier pickle
all_data.to_pickle("data/all_flow_data.pkl")

# Vérification
print("Nombre total d'enregistrements:", len(all_data))

# pd.set_option("display.max_columns", None)
# pd.set_option("display.max_rows", None)
