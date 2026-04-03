# NYC Taxi Analytics — Microsoft Fabric BI Pipeline

An end-to-end Business Intelligence project built on **Microsoft Fabric** that ingests NYC Yellow Taxi trip data, cleans and transforms it through a multi-layer warehouse architecture, and delivers insights via a **Power BI** report backed by a semantic model.

---

## Overview

This project implements a production-style data pipeline following the **Staging → Presentation** pattern, with full metadata logging at every stage. The pipeline is designed to be fully dynamic — it automatically detects the latest loaded date and increments month-over-month without manual intervention.

```
Parquet Files (NYC TLC)
        ↓
[Copy Data Pipeline] — dynamic filename via concat + date variable
        ↓
STG.NYC_Taxi_Yellow  (Lakehouse / Data Warehouse — Staging)
        ↓
[Stored Procedure] — outlier date removal, dynamic start/end dates
        ↓
[Dataflow Gen2] — column cleanup, borough/zone pivot, conditional columns
        ↓
dbo.nyctaxi_yellow_proc  (Presentation Layer)
        ↓
Semantic Model → Power BI Report
```

Metadata is captured at both the staging and presentation layers into `metadata.processing_log`.

---

## Platform & Tech Stack

| Component | Technology |
|---|---|
| Platform | Microsoft Fabric |
| Storage | Fabric Lakehouse |
| Warehouse | Fabric Data Warehouse |
| Orchestration | Data Pipeline (Copy Activity, Stored Procedure Activity, Script Activity, Set Variable) |
| Transformation | Dataflow Gen2 (Power Query) |
| Reporting | Power BI (Semantic Model + Report) |
| Language | T-SQL |
| Data Source | NYC TLC Yellow Taxi `.parquet` files |

---

## Repository Structure

```
├── pipelines/
│   ├── nyc_yellow_ingestion_pipeline    # Copy + clean + metadata (staging)
│   ├── nyc_presentation_pipeline        # Dataflow Gen2 + metadata (presentation)
│   └── nyc_master_pipeline              # Invokes both pipelines in sequence
├── stored_procedures/
│   ├── STG.nyc_yellow_datacleaning.sql
│   ├── metadata.processing_stProc.sql
│   └── metadata.insert_presentation_metadata.sql
├── tables/
│   ├── metadata.processing_log.sql
│   └── dbo.nyctaxi_yellow_proc.sql
└── README.md
```

---

## Data Source

**NYC TLC Yellow Taxi Trip Records** — monthly Parquet files publicly available from the NYC Taxi & Limousine Commission.

[Data Source](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

File naming convention:
```
yellow_tripdata_YYYY-MM.parquet
```

The pipeline dynamically constructs the filename using:
```
@concat('yellow_tripdata_', variables('Date'), '.parquet')
```

---

## Pipeline Walkthrough

### Stage 1 — Staging Ingestion Pipeline

**Copy Data Activity**

Loads the monthly Parquet file into `STG.NYC_Taxi_Yellow` in the Data Warehouse. The filename is dynamically generated using a `Date` variable and the `concat()` function, so no hardcoding is needed per run.

**Dynamic Date Allocation (Script Activity)**

Before each run, the pipeline queries the metadata log to find the last successfully processed month:

```sql
SELECT TOP 1 latest_processed_pickup
FROM metadata.processing_log
WHERE table_processed = 'NYC_Taxi_Yellow'
ORDER BY latest_processed_pickup DESC;
```

The result is assigned to the `Date` variable using a Set Variable activity with the expression:

```
@formatDateTime(
  addToTime(
    activity('DynamicDateAllocation').output.resultSets[0].rows[0].latest_processed_pickup,
    1, 'Month'
  ),
  'yyyy-MM'
)
```

This automatically advances to the next month on every run.

**Data Cleaning — Stored Procedure**

Removes records with pickup datetimes outside the valid window for the loaded month. Parameters make it fully reusable across months:

```sql
CREATE PROCEDURE STG.nyc_yellow_datacleaning
    @start_date DATETIME2,
    @end_date   DATETIME2
AS
    DELETE FROM STG.NYC_Taxi_Yellow
    WHERE tpep_pickup_datetime < @start_date
       OR tpep_pickup_datetime > @end_date;
```

The `@end_date` is dynamically computed by adding 1 month to the start date:
```
@addToTime(concat(variables('Date'), '-01'), 1, 'Month')
```

**Metadata Logging — Stored Procedure**

After each successful load, pipeline run details are written to `metadata.processing_log`:

```sql
CREATE PROCEDURE metadata.processing_stProc
    @pipeline_run_id VARCHAR(255),
    @table_name      VARCHAR(255),
    @processed_date  DATETIME2
AS
    INSERT INTO metadata.processing_log
      (pipeline_run_id, table_processed, rows_processed,
       latest_processed_pickup, processed_datetime)
    SELECT
        @pipeline_run_id,
        @table_name,
        COUNT(*),
        MAX(tpep_pickup_datetime),
        @processed_date
    FROM STG.NYC_Taxi_Yellow;
```

---

### Stage 2 — Presentation Pipeline

**Dataflow Gen2**

Reads from the staging schema and applies the following transformations:
- Removes unwanted columns
- Adds a conditional column for business categorisation
- Joins the taxi zone lookup (borough/zone) via left join pivot
- Writes the cleaned, enriched data to `dbo.nyctaxi_yellow_proc`

**Presentation Table Schema**

```sql
CREATE TABLE dbo.nyctaxi_yellow_proc (
    vendor                  VARCHAR(50),
    tpep_pickup_datetime    DATE,
    tpep_dropoff_datetime   DATE,
    pu_borough              VARCHAR(100),
    pu_zone                 VARCHAR(100),
    do_borough              VARCHAR(100),
    do_zone                 VARCHAR(100),
    payment_method          VARCHAR(50),
    passenger_count         INT,
    trip_distance           FLOAT,
    total_amount            FLOAT
);
```

**Metadata Logging — Presentation Layer**

A second stored procedure logs metadata after the Dataflow Gen2 completes:

```sql
CREATE PROCEDURE metadata.insert_presentation_metadata
    @pipeline_run_id VARCHAR(255),
    @table_name      VARCHAR(255),
    @processed_date  DATETIME2
AS
    INSERT INTO metadata.processing_log
      (pipeline_run_id, table_processed, rows_processed,
       latest_processed_pickup, processed_datetime)
    SELECT
        @pipeline_run_id,
        @table_name,
        COUNT(*),
        MAX(tpep_pickup_datetime),
        @processed_date
    FROM dbo.nyctaxi_yellow;
```

---

### Stage 3 — Master Pipeline

A parent pipeline invokes both the staging and presentation pipelines in sequence using **Execute Pipeline** activities, providing a single trigger point for the full end-to-end run.

---

## Metadata Tracking

All pipeline runs are logged to a central metadata table for auditability and incremental load management:

```sql
CREATE TABLE metadata.processing_log (
    pipeline_run_id         VARCHAR(255),
    table_processed         VARCHAR(255),
    rows_processed          INT,
    latest_processed_pickup DATETIME2(6),
    processed_datetime      DATETIME2(6)
);
```

This table serves two purposes:
- **Audit trail** — every pipeline run is recorded with row counts and timestamps
- **Incremental load driver** — the `latest_processed_pickup` column is queried to dynamically determine the next month to load

---

## Semantic Model & Power BI Report

Once data lands in `dbo.nyctaxi_yellow_proc`, it is published as a **Fabric Semantic Model** (formerly Power BI Dataset). A blank report is created directly from the semantic model within the Fabric workspace.

The report provides analytics across:
- Trip volume trends over time
- Revenue and fare analysis
- Pickup and drop-off borough/zone breakdowns
- Passenger count and trip distance distributions
- Payment method splits

---

## Setup & Execution

### Prerequisites

- Microsoft Fabric workspace with a Lakehouse and Data Warehouse attached
- Access to NYC TLC Yellow Taxi Parquet files
- Schemas pre-created: `STG`, `DBO`, `metadata`

### Steps

1. **Create schemas** in the Data Warehouse:
   ```sql
   CREATE SCHEMA STG;
   CREATE SCHEMA metadata;
   ```

2. **Create tables** — run `metadata.processing_log.sql` and `dbo.nyctaxi_yellow_proc.sql`.

3. **Deploy stored procedures** — run all three `.sql` files in the `stored_procedures/` folder.

4. **Set up the ingestion pipeline** — configure the Copy Data activity with the dynamic filename expression and wire up all stored procedure and script activities.

5. **Set up the Dataflow Gen2** — connect to the staging table, apply transformations, and set the destination to `dbo.nyctaxi_yellow_proc`.

6. **Set up the master pipeline** — add Execute Pipeline activities pointing to the ingestion and presentation pipelines.

7. **Trigger the master pipeline** — set the initial `Date` variable to the first month you want to load (e.g., `2024-01`). Subsequent runs will auto-increment.

8. **Publish the semantic model** and create a Power BI report from the Fabric workspace.

---

## Key Design Decisions

- **Dynamic date handling** — no hardcoded dates anywhere in the pipeline; the `Date` variable drives everything and self-updates via metadata.
- **Stored procedures over direct deletes** — encapsulates cleaning logic and makes it reusable and parameterised.
- **Metadata logging at every layer** — provides full observability into what was loaded, when, and how many rows.
- **Dataflow Gen2 for transformation** — Power Query's no-code/low-code interface speeds up column mapping and join logic without writing custom Spark code.
- **DirectLake-ready presentation layer** — the `dbo` schema table feeds directly into the semantic model for fast Power BI refresh.

---


