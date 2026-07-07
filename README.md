# ETL Pipeline: Postgres to ClickHouse

This project implements an ETL pipeline to transfer data from **PostgreSQL** to **ClickHouse** using **Apache Airflow** and **Apache Spark**. The pipeline leverages **MinIO** for raw and transformed storage in Parquet format.

---

## Architecture Overview

```mermaid
flowchart LR
    subgraph Sources
        PG[(PostgreSQL)]
    end

    subgraph Airflow["Airflow (Orchestrator)"]
        direction TB
        E(Trigger Extract Job)
        DQ1(Staging Data Quality)
        T(Trigger Transform Job)
        L(Trigger Load Job)
    end

    subgraph Processing["Spark (Processor)"]
        direction TB
        Extract(Spark Extract)
        Transform(Spark Transform)
        Load(Spark Load)
    end

    subgraph MinIO["MinIO (Storage)"]
        S1[(Staging Parquet)]
        S2[(Transformed Parquet)]
    end

    subgraph Target
        CH[(ClickHouse)]
    end

    %% Data flow connections
    PG --> |Extract| E
    E --> Extract
    Extract --> |Validate| DQ1
    DQ1 --> |Write| S1
    S1 --> |Read| T
    T --> |Process| Transform
    Transform --> |Write| S2
    S2 -->|Read| L
    L --> Load
    Load -->|Load| CH

    classDef sourceTarget stroke-width:2px;
    classDef processing stroke-width:2px;
    classDef storage stroke-width:2px;
    class PG,CH sourceTarget
    class Airflow,Processing processing
    class Storage storage
```

---

## Components

- **PostgreSQL**: Source transactional database.
- **Apache Airflow**: Orchestrates the ETL workflow.
- **Apache Spark**: Handles data extraction, transformation, and loading.
- **MinIO**: Stores intermediate Parquet files.
- **ClickHouse**: Analytics-optimized target database.

---

## ETL Steps

1. **Extract**: Data is read from PostgreSQL using Spark and saved as Parquet in MinIO.
2. **Data Quality**: Checks are performed on staging data.
3. **Transform**: Data is transformed using Spark and saved as Parquet in MinIO.
4. **Load**: Transformed data is loaded into ClickHouse.

---

## Technologies Used

- Apache Airflow
- Apache Spark
- PostgreSQL
- ClickHouse
- MinIO
- Docker

---

## Getting Started

1. Clone the repository.
2. Open terminal / command prompt and change directory to the project folder (`deso-query/`)
3. Build Airflow image with `docker build -t custom-airflow:latest -f Dockerfile.airflow .`
4. Start services with `docker-compose up -d`.
5. Trigger the ETL DAG in Airflow.

---

For more details, see the config-driven bronze ingestion in [dags/dag_bronze_postgres.py](dags/dag_bronze_postgres.py) (and `dag_bronze_mysql` / `dag_bronze_api`) and the dbt silver/gold transforms in [dags/dag_silver_gold_dbt.py](dags/dag_silver_gold_dbt.py).