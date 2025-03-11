# ETL Pipeline: Postgres to ClickHouse

```mermaid
flowchart LR
    subgraph Sources
        PG[(PostgreSQL)]
    end

    subgraph Airflow["Apache Airflow"]
        direction TB
        E[ PySpark Extract ]
        DQ1[ Staging Data Quality ]
        T[Transform]
        L[Load]
    end

    subgraph Processing["Data Processing"]
        direction TB
        Spark[ PySpark Engine ]
    end

    subgraph Storage
        S1[(Staging
		        Parquet)]
        S2[(Processed
		        Parquet)]
    end

    subgraph Target
        CH[(ClickHouse)]
    end

    %% Data flow connections
    PG --> |Extract| E
    E --> |Validate| DQ1
    DQ1 --> |Write| S1
    S1 --> |Read| T
    T --> |Process| Spark
    Spark --> |Write| S2
    S2 -->|Read| L
    L -->|Load| CH

    class PG,CH sourceTarget
    class Airflow,Processing processing
    class Storage storage