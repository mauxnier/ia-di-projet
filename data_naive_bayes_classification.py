from elasticsearch import Elasticsearch
import pandas as pd
from itertools import combinations
from sklearn.calibration import LabelEncoder
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import classification_report, accuracy_score, f1_score, precision_score, recall_score

# Specify the Elasticsearch node URLs (can be a single or list of nodes)
hosts = ["http://localhost:9200"]

# Connect to Elasticsearch
es = Elasticsearch(hosts=hosts)

# Apply transport options to the Elasticsearch object
es.options(request_timeout=60, max_retries=5, retry_on_timeout=True)

# Define Elasticsearch index
index_name = "flow_data_enc"

# Query to retrieve data
query = {
    "bool": {
        "should": [
            {"term": {"appName_HTTPWeb": "true"}},
            {"term": {"appName_SSH": "true"}},
        ],
        "minimum_should_match": 1,
    }
}

# Execute the query
result = es.search(
    index=index_name,
    query=query,
    scroll="2m",
    size=10000,
)

# Initialize an empty DataFrame to store the results
df_list = []

# Continue scrolling until no more results
while len(result["hits"]["hits"]) > 0:
    # Process the current batch of results and create the DataFrame
    df = pd.DataFrame(hit["_source"] for hit in result["hits"]["hits"])
    df_list.append(df)

    # Use the scroll ID to retrieve the next batch
    result = es.scroll(scroll_id=result["_scroll_id"], scroll="2m")

# Close the scroll
es.clear_scroll(scroll_id=result["_scroll_id"])

print("df_list.size x 10.000: ", len(df_list))

# Concatenate all the results into a single DataFrame
df = pd.concat(df_list, axis=0)

# Print the new DataFrame shape after dropping NaN values
print("Dataframe shape after dropping NaN values:", df.shape)

# Check if there are still samples in the dataset after dropping NaN values
if df.shape[0] == 0:
    print("No samples remaining in the dataset after dropping NaN values.")
else:
    # Check for NaN values in the DataFrame
    nan_counts = df.isna().sum()
    print("Number of NaN values in each column:")
    print(nan_counts)

    # Identify columns with NaN values
    columns_with_nan = nan_counts[nan_counts > 0].index.tolist()
    print("Columns with NaN values:", columns_with_nan)

    # Choose whether to impute or drop NaN values in those columns
    # Example: Impute with mean
    imputer = SimpleImputer(strategy='mean')
    df[columns_with_nan] = imputer.fit_transform(df[columns_with_nan])

# Define the features and target variable for classification
X = df.drop(['Tag_Attack', 'Tag_Normal'], axis=1)
y = df['Tag_Attack']

# Convert the target variable to categorical
y = pd.Categorical(y)

# Train-Test Split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# Further split the training set into four subsets for training
train_combinations = list(combinations([X_train, y_train], 4))

# Preprocess Data (Standardization)
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Convert labels to numeric values
le = LabelEncoder()
y_train = le.fit_transform(y_train)
y_test = le.transform(y_test)

# Convert labels to binary integers
y_train_binary = y_train.astype(int)
y_test_binary = y_test.astype(int)

# Train Naive Bayes Model and evaluate on all combinations
nb_model = GaussianNB()

try:
    for i, train_combination in enumerate(train_combinations):
        X_train_subset, y_train_subset, _, _ = train_test_split(
            train_combination[0], train_combination[1], train_size=0.2, random_state=42
        )

        X_train_subset_scaled = scaler.transform(X_train_subset)
        
        # Convert labels to binary integers for the subset
        y_train_subset_binary = y_train_subset.astype(int)
        
        # Train Naive Bayes Model for the subset
        nb_model.fit(X_train_subset_scaled, y_train_subset_binary)
        y_pred_subset = nb_model.predict(X_test_scaled)

        # Convert labels to binary integers for the test set
        y_test_binary = y_test.astype(int)
        
        # Calculate accuracy, precision, recall, and F1-score
        accuracy_subset = accuracy_score(y_test_binary, y_pred_subset)
        precision_subset = precision_score(y_test_binary, y_pred_subset, average='weighted', zero_division=1)
        recall_subset = recall_score(y_test_binary, y_pred_subset, average='weighted', zero_division=1)
        f1_subset = f1_score(y_test_binary, y_pred_subset, average='weighted', zero_division=1)

        print(f"\nNaive Bayes Classification Report on Training Combination {i + 1}:")
        print(f"Accuracy on Training Combination {i + 1}: {accuracy_subset}")
        print(f"Precision on Training Combination {i + 1}: {precision_subset}")
        print(f"Recall on Training Combination {i + 1}: {recall_subset}")
        print(f"F1-score on Training Combination {i + 1}: {f1_subset}")

except ValueError as e:
    print(f"Error fitting the model: {e}")

# Fit the model on the full training set
X_train_scaled_full = scaler.transform(X_train)

# Convert labels to binary integers for the full training set
y_train_binary_full = y_train.astype(int)

# Train Naive Bayes Model on the full training set
nb_model.fit(X_train_scaled_full, y_train_binary_full)

# Print overall accuracy, precision, recall, and F1-score on the full training set
y_pred_full = nb_model.predict(X_test_scaled)
accuracy_full = accuracy_score(y_test_binary, y_pred_full)
precision_full = precision_score(y_test_binary, y_pred_full, average='weighted', zero_division=1)
recall_full = recall_score(y_test_binary, y_pred_full, average='weighted', zero_division=1)
f1_full = f1_score(y_test_binary, y_pred_full, average='weighted', zero_division=1)

print("\nOverall Metrics on the Full Training Set:")
print(f"Accuracy: {accuracy_full}")
print(f"Precision: {precision_full}")
print(f"Recall: {recall_full}")
print(f"F1-score: {f1_full}")
