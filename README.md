# ELT-DataLake
This project contains the implementation of a simple ETL pipeline from a traditional RDBMS to an object storage-based data lake, with data transformation using dataframes, storage in Parquet format, and querying using a lightweight SQL engine.

## 🧱 Medallion Architecture Overview

This project follows the **Medallion Architecture** pattern for organizing data processing stages in the data lake. It consists of three layers:

### 🥉 Bronze Layer (Raw Data)
- Contains raw, unvalidated, or lightly structured data.
- Data is ingested directly from the source: either a relational database or CSV files.
- Purpose: **Data landing zone** — captures everything without modification.

📄 In this project: `bronze.py`  
→ Extracts data and uploads it as-is to MinIO in Parquet format.

---

### 🥈 Silver Layer (Cleansed Data)
- Data is cleaned, deduplicated, and validated.
- May include standardization of formats, handling of missing values, and normalization.
- Purpose: **Intermediate zone** — data is trustworthy and ready for analysis.

📄 In this project: `silver.py`  
→ Loads bronze data, transforms it using Pandas, and writes clean data to the silver layer.

---

### 🥇 Gold Layer (Curated & Modeled Data)

- Fully transformed, aggregated, and structured data designed for **analytics and querying**.
- In this project, the **Gold Layer is modeled as a Star Schema** — a common structure in data warehousing that separates:
  - **Fact tables**: Contain measurable, quantitative data (e.g., transactions, sales).
  - **Dimension tables**: Contain descriptive attributes (e.g., time, product, customer).

📄 In this project: `gold.py`  
→ Loads the cleansed Silver data, applies business logic, and outputs the data as **Parquet files in Star Schema format**, ready to be queried by SQL engines like DuckDB.

---

### 📌 Why Star Schema?

- **Optimized for querying**: Simplifies SQL queries and boosts performance.
- **Separation of metrics and context**: Makes data easier to understand and analyze.
- **Scalable structure**: Works well in both small-scale and enterprise data lakes.

---

### 📌 Benefits of the Medallion Architecture

- **Layered abstraction**: Makes it easier to debug, trace, and manage data transformations.
- **Data quality gates**: Ensures poor-quality data doesn’t affect downstream analysis.
- **Scalable & modular**: Each layer can be rerun or updated independently.
- **Separation of concerns**: Raw, processed, and ready-to-use data are clearly separated.

---

🧭 This structure mirrors how modern data lakes (e.g., on Databricks, Delta Lake) operate and is well-suited for reproducible, auditable ELT pipelines — even in lightweight environments using MinIO + DuckDB.


## 🛠 Initial Setup

### 1. Prepare the Source Data

You can choose one of the following approaches depending on your preferred data source:

#### If You Use a Database (MySQL / MariaDB):

1. Install a local MySQL or MariaDB server.
2. Execute the SQL scripts inside `/source_data/SQL` to create the schema and populate the database. 
   
    ```bash
    mysql -u root -p < source_data/SQL/SourceScript.sql
    ```
3. Update the ELT pipeline configuration (e.g. .env or Python config file) with your database connection details.

#### If You Use CSV Files:

1. Ensure the CSV files are available under /source_data/CSV.
2. Modify the code to read from CSV files instead of connecting to a database.
   Example (in Python):

    ```python
    import pandas as pd
    df = pd.read_csv("source_data/CSV/mahasiswa.csv")    
    ```

### 2. Setup MinIO (Object Storage)

- Download and install the [MinIO Server](https://min.io/download) according to your operating system.
- Start the server based on your OS instructions:

  - **Windows**:
    ```bash
    minio.exe server C:\Path\to\your\server --console-address ":9001"
    ```

  - **Linux/macOS**:
    ```bash
    minio server /path/to/your/server --console-address ":9001"
    ```

- After starting, open your browser and visit:  
  **http://localhost:9001** to access the MinIO Console UI.
- Login using default credentials:
  - **Username**: `minioadmin`
  - **Password**: `minioadmin`

> It is recommended to change the default username and password!

*Make sure MinIO is running and accessible before proceeding to the ELT pipeline.*

## 🛠 Time to EXTRACT LOAD and TRANSFORM baby...

### Run the ETL Pipeline
Once the data source and MinIO server are ready, run the ELT script:
```bash
python ELT_pipeline.py
```
This script automates the execution of the ELT pipeline stages (bronze.py, silver.py, and gold.py) in order, while also:
- Measuring execution time for each stage
- Monitoring and logging peak RAM usage

## 🧾 Querying and Viewing Parquet Data from MinIO

### 📌 1. `query.py` — Run SQL Queries on MinIO-based Parquet Files Using DuckDB
  This script allows you to execute predefined SQL queries directly on **Parquet files stored in MinIO**, using **DuckDB with S3 integration**.
  - Connects DuckDB to MinIO via `httpfs` (no need to download data locally).
  - Runs SQL queries on `gold` layer Parquet tables.
  - Supports joining fact and dimension tables.
  - Returns results to the console.
  
  #### 🧰 Requirements: DuckDB Python API
      pip install duckdb
      
  #### 🧪 Usage: 
    
      python query.py --query 2

  #### ✏️ Customize Your Own Query:
  You can easily modify or add new queries to the queries dictionary in query.py:
  ```python
  queries = {
    "6": "SELECT COUNT(*) FROM read_parquet('s3://gold/frs_fact.parquet');"
  }
  ```
  Then run:
  ```bash
  python query.py --query 6
  ```
  > 🔧 This makes the script flexible for exploratory analysis or integrating business-specific queries.
  
### 👁️ 2. view.py — View Tables from MinIO (Bronze, Silver, or Gold)
  This script enables you to quickly inspect tables stored as Parquet files in MinIO across bronze, silver, and gold layers.
  - Reads Parquet files directly from MinIO without downloading them.
  - Displays the first few rows of each table using pandas.
  - Supports viewing all tables in a layer, or a specific table.

#### 🧰 Requirements:
    pip install minio pandas pyarrow

#### 🧪 Usage:
    # View all tables in the silver layer
    python view.py --layer silver --table all
    
    # View specific table
    python view.py --layer gold --table dim_mahasiswa

### 🔐 MinIO Access Configuration
Both scripts assume default credentials and endpoint:
- Endpoint: localhost:9000
- Access Key: minioadmin
- Secret Key: minioadmin
- Secure: False
Update as needed in the script or environment for production use.
