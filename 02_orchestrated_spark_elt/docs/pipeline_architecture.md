# Orchestrated Spark ELT - Pipeline Architecture

This diagram highlights Prefect as the central orchestrator controlling both tasks and monitoring run state.

```mermaid
flowchart TB
    PF["Prefect Orchestrator\norchestrated_spark_elt_flow"]

    subgraph Source[Source Data]
        D["dataset folder\nCSV files"]
    end

    subgraph Bronze[Bronze Layer - Minio]
        U["Task: upload_to_minio"]
        M["Minio"]
        B["Bucket: bronze/dataset.csv"]
    end

    subgraph Processing[Processing Layer - Local Spark]
        R["Task: run_data_processing"]
        S["SparkSession\nlocal all cores"]
        X["Clean step\ndropDuplicates + dropna"]
        A["S3A connector"]
        J["JDBC connector"]
    end

    subgraph Silver[Silver Layer - PostgreSQL]
        P[(PostgreSQL)]
        T[(silver_cleaned_data\noverwrite)]
    end

    subgraph Control[Control Plane]
        PS["Prefect Server UI/API"]
    end

    PF -->|control step 1| U
    PF -->|control step 2| R

    D --> U
    U --> M --> B

    R --> S
    B --> A --> S
    S --> X --> J --> P --> T

    PF -. run state and logs .-> U
    PF -. run state and logs .-> R
    PF -. metadata and monitoring .-> PS
```

Edit/preview link: https://l.mermaid.ai/gMZV0y
