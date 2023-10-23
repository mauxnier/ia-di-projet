# from pyspark.sql import SparkSession
# from pyspark.ml.classification import MultilayerPerceptronClassifier
# from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# from pyspark.ml.feature import VectorAssembler

# # Initialisez SparkSession
# spark = SparkSession.builder.appName("MalwareClassification").getOrCreate()
# data = spark.read.format("your_data_format").option("option_name", "option_value").load("path/to/your/data")

# # Chargez les images et effectuez le prétraitement
# # ...

# # Créez un DataFrame Spark à partir des données prétraitées
# # ...

# # Assemblez les fonctionnalités en un vecteur
# feature_cols = ["feature1", "feature2","feature3"]
# assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
# assembled_data = assembler.transform(data)

# # Divisez les données en ensembles d'entraînement et de test
# train_data, test_data = assembled_data.randomSplit([0.8, 0.2], seed=123)

# # Créez un modèle Multilayer Perceptron Classifier
# layers = [len(feature_cols), 128, 64, 2]  # Adapté à votre problème
# classifier = MultilayerPerceptronClassifier(layers=layers, blockSize=128, seed=1234, labelCol="label")

# # Entraînez le modèle
# model = classifier.fit(train_data)

# # Faites des prédictions sur l'ensemble de test
# predictions = model.transform(test_data)

# # Évaluez les performances du modèle
# evaluator = MulticlassClassificationEvaluator(metricName="accuracy")
# accuracy = evaluator.evaluate(predictions)
# print("Accuracy = {:.2f}%".format(accuracy * 100))

# # Sauvegardez le modèle entraîné
# model.save("/home/ikrame/src")

from data_loading import all_flow_data
import pandas as pd

# Supposons que 'all_flow_data' soit une liste de dictionnaires où chaque dictionnaire représente un flow
# Par exemple, un élément de 'all_flow_data' ressemblerait à ceci :
# flow_data = {
#     'appName': 'Unknown_UDP',
#     'totalSourceBytes': 16076,
#     'totalDestinationBytes': 0,
#     # ... d'autres champs ...
# }

# Créez un DataFrame à partir de 'all_flow_data'
# df = pd.DataFrame(all_flow_data)

# Affichez le DataFrame
# print(df.head())

# Utilisez la fonction get_dummies de pandas pour effectuer one-hot encoding
# df_encoded = pd.get_dummies(df, columns=['application', 'protocolFields'])

# Le DataFrame 'df_encoded' contient maintenant les colonnes one-hot encoded pour 'application' et 'protocolFields'

# Affichez le DataFrame résultant
# print(df_encoded.head())
