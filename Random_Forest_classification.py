from elasticsearch import Elasticsearch
import pandas as pd
from sklearn.calibration import LabelEncoder
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score

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

# Preprocess Data (Standardization)
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Convert labels to numeric values
le = LabelEncoder()
y_train = le.fit_transform(y_train)
y_test = le.transform(y_test)

# Train Random Forest Model
rf_model = RandomForestClassifier(n_estimators=100, random_state=42)

try:
    rf_model.fit(X_train_scaled, y_train)
    y_pred = rf_model.predict(X_test_scaled)

    # Evaluate Performance
    accuracy = accuracy_score(y_test, y_pred)
    classification_report_result = classification_report(
        y_test, y_pred, zero_division=1
    )

    print("\nRandom Forest Classification Report:")
    print(classification_report_result)
    print("Accuracy:", accuracy)

except ValueError as e:
    print(f"Error fitting the model: {e}")
