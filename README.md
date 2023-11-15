# AD4IDS - Anomaly Detection for Intrusion Detection Systems

## Subproject: 1 - Flow Classification

### Overview
This project focuses on flow classification for Intrusion Detection Systems (IDS). It includes different stages such as data loading, preprocessing (one-hot encoding and K-Nearest Neighbors classification), and indexing in Elasticsearch. The goal is to enhance the efficiency of intrusion detection through machine learning techniques.

### Authors
- **MONNIER Killian**
- **BAKKARI Ikrame**

### Project Structure
- **Stage 1: Data Loading from XML files**
  - Parses XML files containing flow data and converts them into a list of dictionaries.
  - Example Usage:
    ```python
    python data_loading.py
    ```

- **Stage 2: Data Indexing in Elasticsearch**
  - Indexes flow data into Elasticsearch for efficient storage and retrieval.
  - Example Usage:
    ```python
    python data_indexing.py
    ```

- **Stage 3: Data Preprocessing (One-Hot Encoding)**
  - Converts categorical data into a one-hot encoded format.
  - Example Usage:
    ```python
    python data_preprocessing.py
    ```

- **Stage 4: Data Preprocessing (K-Nearest Neighbors Classification)**
  - Applies K-Nearest Neighbors classification to the preprocessed data.
  - Example Usage:
    ```python
    python data_preprocessing_knn.py
    ```

### Prerequisites
- Python 3.x
- Elasticsearch instance running locally

### Installation
1. Clone the repository:
    ```bash
    git clone https://github.com/mauxnier/ia-di-projet.git
    cd ia-di-projet
    ```

### Usage
1. Ensure your Elasticsearch instance is running.
2. Execute the desired stage script:
    ```bash
    python script_name.py
    ```

### Configuration
- Elasticsearch Configuration:
  - Modify the Elasticsearch node URLs in the scripts if needed.
  - Adjust index names and other Elasticsearch configurations as necessary.

==

