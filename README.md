# Metadata-Driven Medallion Data Pipeline

An end-to-end ELT pipeline moving retail data through Bronze, Silver and Gold
layers on Azure, orchestrated in Azure Data Factory with transformations in
Databricks (PySpark).

The pipeline is **metadata-driven**: what gets ingested, from where, in what
format and to which Gold object is all defined in a SQL control table. Adding a
new source file means inserting a row, not editing the pipeline.

---

## Architecture

```
Source (raw drop)
      │
      │  ADF Copy activity, driven by control table
      ▼
Bronze  ─── raw files, unmodified
      │
      │  Databricks notebook (PySpark)
      ▼
Silver  ─── validated, typed, cleansed Parquet
      │
      │  Databricks notebook (PySpark)
      ▼
Gold    ─── Delta star schema
```

Every layer transition writes an audit row to a SQL logging table with row
counts, timings, status and error detail.

---

## Stack

| Component | Used for |
|---|---|
| Azure Data Factory | Orchestration, scheduling, retry, error handling |
| Azure Databricks | PySpark transformations |
| Azure Data Lake Storage Gen2 | Bronze / Silver / Gold storage |
| Azure SQL Database | Control table, audit log, logging stored procedure |
| Delta Lake | Gold layer table format |

---

## Repository layout

```
/notebooks
    bronze_to_silver.py      Validation, cleansing, transformations
    silver_to_gold.py        Dimensional modelling, Delta writes
/sql
    01_control_table.sql     Control table DDL + seed rows
    02_logging_table.sql     Audit table DDL
/adf
    pipeline.json            Exported pipeline definition
    datasets/                Parameterised dataset definitions
    linkedservices/          Linked service definitions
/docs
    pipeline_diagram.png
```

---

## The control table

The pipeline reads this once per run and iterates over its rows.

| Column | Purpose |
|---|---|
| id | Surrogate key |
| sourcefilename | File to ingest |
| file_format | csv or json — drives conditional routing |
| delimiter | Column delimiter for delimited sources |
| bronze_path | Target folder in Bronze |
| silver_path | Target folder in Silver |
| gold_objectname | Gold object this source feeds |

Seed data:

| sourcefilename | file_format | delimiter | bronze_path | silver_path | gold_objectname |
|---|---|---|---|---|---|
| customers.csv | csv | , | bronze | silver/customers | DimCustomer |
| products.csv | csv | , | bronze | silver/products | DimProduct |
| orders.csv | csv | , | bronze | silver/orders | FactSales |
| survey.json | json | NULL | bronze | silver/survey | FactSales |

Onboarding a fifth source is one INSERT. No pipeline edit, no new dataset.

---

## Orchestration

```
Lookup (control table)
   └─► ForEach (sequential)
          ├─ Set start timestamp
          ├─ If file_format = csv
          │     ├─ true  → Copy via parameterised DelimitedText dataset
          │     └─ false → Copy via Binary dataset (preserves JSON byte-for-byte)
          └─ Log success / log failure → Fail
   └─► Notebook: Bronze → Silver
          └─ Log success / log failure → Fail
   └─► Notebook: Silver → Gold
          └─ Log success / log failure → Fail
```

Design decisions worth noting:

**Notebooks sit outside the loop.** The ForEach iterates per *file*; the
transformation notebooks operate on the *layer*. Putting them inside the loop
would run each notebook once per control row, reprocessing every file each time
and writing concurrently to the same paths.

**JSON is copied as Binary, not as JSON.** An ADF JSON source into a JSON sink
re-serialises the file and defaults to a JSON Lines layout, which breaks a
downstream `multiline=true` read. Binary copies the bytes unchanged, which is
also what "Bronze holds raw files" should mean.

**Every failure path ends in a Fail activity.** A red path that terminates in a
successful logging procedure makes ADF report the whole pipeline as Succeeded —
the error gets logged and then silently swallowed.

**Lookup returns all rows.** `firstRowOnly` defaults to true, which returns a
single object with no `value` array and breaks the ForEach before it starts.

---

## Bronze → Silver

Explicit `StructType` schemas rather than `inferSchema`, which samples and is
not deterministic across runs.

**Data quality quarantine.** Records failing validation are filtered into
separate `*_bad.parquet` outputs and removed from the good frames, so bad rows
never reach Gold:

| Dataset | Rejection rules |
|---|---|
| customers | null customer_id, null or malformed email |
| products | null product_id |
| orders | negative or null quantity, negative or null price |

**Transformations.** Missing `city` defaults to `Unknown` and missing
`product_category` to `Misc`; `total_value` is computed as quantity × price; a
registered PySpark UDF applies a 10% discount to orders from loyalty customers,
producing `discounted_amount`; the survey JSON is flattened and cast to typed
columns.

Each frame writes to its own Silver folder as Parquet.

---

## Silver → Gold

Reads Silver Parquet and produces a star schema in Delta:

| Object | Grain | Source |
|---|---|---|
| DimCustomer | one row per customer | customers |
| DimProduct | one row per product | products |
| FactSales | one row per order | orders joined to survey |

---

## Audit logging

ADF cannot insert into a SQL table directly, so logging goes through a stored
procedure, `Metadata.usp_log_event`, called by Stored procedure activities on
both the success and failure path of every data movement.

| Column | Captured |
|---|---|
| run_id | ADF pipeline run ID |
| source_name, source_layer, target_layer | What moved, and between which layers |
| source_path, target_path | Resolved paths for the run |
| start_time, end_time | Duration |
| status | Success or Failed |
| records_processed | Rows copied, or notebook exit value |
| error_message | Activity error text on failure |

A clean run produces six rows: four Source → Bronze, one Bronze → Silver, one
Silver → Gold.

Row counts from the notebooks come back via `dbutils.notebook.exit()`, read in
ADF as `@activity('<name>').output.runOutput`.

---

## Reliability

Activity-level retries (2 for copies, 1 for notebooks — a notebook failing on a
code error fails identically on retry and each attempt costs cluster time),
explicit timeouts, failure paths on every activity, and a daily schedule trigger
at 06:00 IST.

---

## Running it

1. Run the three scripts in `/sql` against an Azure SQL database.
2. Create linked services for ADLS Gen2, Azure SQL and Databricks.
3. Import `/adf/pipeline.json` along with the dataset and linked service definitions.
4. Import the notebooks into a Databricks workspace.
5. Drop the source files into the `source/` folder of the container.
6. Debug the pipeline, then verify:

```sql
SELECT * FROM Metadata.loggingtable ORDER BY log_id;
```

---

## Notes

Storage credentials are read from a Databricks secret scope backed by Key Vault.
No keys are committed to this repository.

On serverless Databricks compute, cluster-level storage configuration is not
available — access is granted through a Unity Catalog external location instead.
