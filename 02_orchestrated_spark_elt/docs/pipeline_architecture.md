# Orchestrated Spark ELT - Pipeline Architecture

Prefect orchestrates three tasks sequentially. CSV source files are ingested automatically into Kafka topics by a producer task, then Spark Structured Streaming lands the raw events in MinIO Bronze, a Spark batch job joins and cleans the data into PostgreSQL Silver, and finally a second Spark batch job computes analytical aggregations in the Gold layer.

```mermaid
flowchart TB
    PF["Prefect Orchestrator\norchestrated_spark_elt_flow"]

    subgraph Source[Source Data]
        D["dataset folder\nflights.csv / airlines.csv / airports.csv"]
    end

    subgraph Queue[Queue Layer - Kafka Broker]
        KP["Task: inject_to_kafka\nkafka_producer.py"]
        T1["Topic: flights\n(3 partitions)"]
        T2["Topic: airlines\n(1 partition)"]
        T3["Topic: airports\n(1 partition)"]
    end

    subgraph Bronze[Bronze Layer - MinIO]
        SS["Task: run_data_processing\nStage 1 – Spark Structured Streaming\ntrigger availableNow=True"]
        B1["s3a://bronze/flights/\nParquet"]
        B2["s3a://bronze/airlines/\nParquet"]
        B3["s3a://bronze/airports/\nParquet"]
    end

    subgraph Silver[Silver Layer - PostgreSQL]
        SB["Stage 2 – Spark Batch\njoin + dropDuplicates + dropna"]
        SV[(silver_cleaned_data\noverwrite)]
    end

    subgraph Gold[Gold Layer - PostgreSQL]
        SG["Stage 3 – Spark Batch\naggregations"]
        G1[(gold_airline_performance)]
        G2[(gold_airport_delay_summary)]
        G3[(gold_delay_by_month_weekday)]
    end

    subgraph Control[Control Plane]
        PS["Prefect Server UI/API"]
    end

    PF -->|"step 1"| KP
    PF -->|"step 2"| SS
    PF -->|"step 3 (gold)"| SG

    D --> KP
    KP --> T1
    KP --> T2
    KP --> T3

    T1 --> SS
    T2 --> SS
    T3 --> SS

    SS --> B1
    SS --> B2
    SS --> B3

    B1 --> SB
    B2 --> SB
    B3 --> SB

    SB --> SV
    SV --> SG

    SG --> G1
    SG --> G2
    SG --> G3

    PF -. run state and logs .-> KP
    PF -. run state and logs .-> SS
    PF -. run state and logs .-> SG
    PF -. metadata and monitoring .-> PS
```


