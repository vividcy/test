# Databricks notebook source
# MAGIC %md
# MAGIC # Dremio-to-Databricks Reconciliation Notebook
# MAGIC
# MAGIC This notebook performs data reconciliation between Dremio (source) and Databricks (target) tables using Lakebridge's custom Dremio connector and reporting utilities.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Restart Python Environment (Optional)
# MAGIC
# MAGIC **Description:**
# MAGIC Restarts the Python interpreter to ensure a clean environment before executing the notebook.
# MAGIC This step clears any previously loaded modules or cached variables from earlier sessions, preventing import or dependency conflicts.
# MAGIC
# MAGIC **Note:**
# MAGIC - Running this cell will reset all variables and imports.
# MAGIC - Wait for the environment to fully restart before proceeding to the next cell.
# MAGIC  
# MAGIC **No input required** — Run as-is.

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load Configuration
# MAGIC
# MAGIC **Description**: Loads the saved configuration from setup notebook to get paths and settings.
# MAGIC
# MAGIC **Required User Input**:
# MAGIC - `VOLUME_PATH`: Volume path defined in the `01_lakebridge_dremio_setup` notebook.

# COMMAND ----------

import sys
import json
from pathlib import Path
from datetime import datetime

# Load saved configuration from volume
VOLUME_PATH = "your-volume-storage-path"  # Storage volume path

config_file = Path(VOLUME_PATH) / "lakebridge-dremio" / "config" / "lakebridge_config.json"
with open(config_file, "r") as f:
    config = json.load(f)

LAKEBRIDGE_APP_PATH = config["lakebridge_path"]
folders = config["folders"]
SECRET_SCOPE = config["secret_scope"]
PROJECT_CATALOG = config["project_catalog"]
PROJECT_SCHEMA = config["project_schema"]

print(f"Configuration loaded from: {config_file}")
print(f"Lakebridge app path: {LAKEBRIDGE_APP_PATH}")
print(f"PROJECT_CATLAOG: {PROJECT_CATALOG}")
print(f"PROJECT_SCHEMA: {PROJECT_SCHEMA}")
print(f"VOLUME: {VOLUME_PATH}")
print(f"SECRET_SCOPE: {SECRET_SCOPE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configure Python Environment
# MAGIC **Description**: Configures environment and imports reconciliation components.
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

import os
import importlib.util

# Set path to Lakebridge-Dremio App
databricks_module_path = f"{LAKEBRIDGE_APP_PATH}/databricks"

if LAKEBRIDGE_APP_PATH not in sys.path:
    sys.path.insert(0, LAKEBRIDGE_APP_PATH)

import databricks
if databricks_module_path not in databricks.__path__:
    databricks.__path__.insert(0, databricks_module_path)

# Import Lakebridge reconciler components
from databricks.labs.lakebridge import __version__
from databricks.labs.lakebridge.reconcile.trigger_recon_service import TriggerReconService
from databricks.labs.lakebridge.reconcile.exception import ReconciliationException
from databricks.labs.lakebridge.config import ReconcileConfig, TableRecon, DatabaseConfig, ReconcileMetadataConfig
from databricks.labs.lakebridge.reconcile.recon_config import (
    Table,
    Filters,
    Transformation,
    SamplingOptions,
    SamplingSpecifications,
    JdbcReaderOptions,
    ColumnMapping,
    AggregateQueryRules,
)
from databricks.labs.lakebridge.reconcile.constants import (
    SamplingOptionMethod,
    SamplingSpecificationsType
)
from databricks.sdk import WorkspaceClient

# Import reporting utilities
from databricks.labs.lakebridge.reconcile.reporting import (
    ReconcileReporter,
    get_recon_summary,
    export_recon_report,
    get_recent_reconciliations
)

print("Lakebridge reconciler components imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Define Reconciliation Connections
# MAGIC
# MAGIC **Description**: Sets source/target schemas and reconciliation parameters.
# MAGIC
# MAGIC **Required User Inputs**:
# MAGIC - `SOURCE_CATALOG`: Your Dremio catalog name
# MAGIC - `SOURCE_SCHEMA`: Your Dremio schema name
# MAGIC - `TARGET_CATALOG`: Your Databricks Unity Catalog name
# MAGIC - `TARGET_SCHEMA`: Your Databricks schema name
# MAGIC
# MAGIC **Optional Inputs**:
# MAGIC - `REPORT_TYPE`: Type of reconciliation report (defaults to "all")
# MAGIC - `METADATA_CATALOG`: Catalog for storing results (defaults to `PROJECT_CATALOG`)
# MAGIC - `METADATA_SCHEMA`: Schema for storing results (defaults to `PROJECT_SCHEMA`)

# COMMAND ----------

# ============================================================================
# CONFIGURATION PARAMETERS - UPDATE THESE FOR YOUR ENVIRONMENT
# ============================================================================

# Data source (using custom Dremio connector)
DATA_SOURCE = "dremio"

# Report type options: "schema", "data", "row", "all", "aggregate"
REPORT_TYPE = "all"

# Source (Dremio) configuration
SOURCE_CATALOG = "DREMIO"  # Replace if different
SOURCE_SCHEMA = "commercial.crm"  # UPDATE: Your Dremio space/schema

# Target (Databricks) configuration
TARGET_CATALOG = "project-catalog"  # UPDATE: Your Databricks catalog
TARGET_SCHEMA = "project-schema"  # UPDATE: Your Databricks schema


# Metadata storage configuration (where reconciliation results are stored)
METADATA_CATALOG = PROJECT_CATALOG  # Matches the default in reporting module
METADATA_SCHEMA = PROJECT_SCHEMA  # Matches the default in reporting module

VOLUME_RECONCILE_PATH = "files/lakebridge-dremio/reconciler_reports"  # Leave of /Volumes/catalog/schema portion of path

print(f"Configuration:")
print(f"  Data source: {DATA_SOURCE}")
print(f"  Report type: {REPORT_TYPE}")
print(f"  Source: Dremio/{SOURCE_SCHEMA}")
print(f"  Target: {TARGET_CATALOG}.{TARGET_SCHEMA}")
print(f"  Metadata: {METADATA_CATALOG}.{METADATA_SCHEMA}")
print(f"  Report Volume: {VOLUME_RECONCILE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Reconiler Configuration Objects
# MAGIC
# MAGIC **Description**: Creates ReconcileConfig, DatabaseConfig, and metadata configuration objects.
# MAGIC
# MAGIC **No input required** - Run as-is (uses values from Cell 3).

# COMMAND ----------

# Create database configuration
database_config = DatabaseConfig(
    source_catalog=SOURCE_CATALOG,
    source_schema=SOURCE_SCHEMA,
    target_catalog=TARGET_CATALOG,
    target_schema=TARGET_SCHEMA
)

# Create metadata configuration for storing reconciliation results
metadata_config = ReconcileMetadataConfig(
    catalog=METADATA_CATALOG,
    schema=METADATA_SCHEMA,
    volume=VOLUME_PATH.split("/")[-1]  # Extract volume name
)

# Create main reconcile configuration
reconcile_config = ReconcileConfig(
    data_source=DATA_SOURCE,
    database_config=database_config,
    metadata_config=metadata_config,
    report_type=REPORT_TYPE,
    secret_scope=SECRET_SCOPE,
    job_id=None  # Will be set if running as a scheduled job
)

print("Configuration objects created")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Define Tables to Reconcile
# MAGIC
# MAGIC - Update the `tables` list with your actual table names:
# MAGIC   - `source_name`: Table name in Dremio
# MAGIC   - `target_name`: Table name in Databricks
# MAGIC   - `join_columns`: Primary key columns for matching records
# MAGIC
# MAGIC **Optional configurations per table**:
# MAGIC - `select_columns`: Specific columns to compare (None = all columns)
# MAGIC - `drop_columns`: Specific columns to be ignored
# MAGIC - `column_mapping`: Map different column names between source and target
# MAGIC - `transformations`: List of `Transformation` objects used to standardize source/target values
# MAGIC - `sampling_options`: For large tables, define sampling parameters
# MAGIC - `jdbc_reader_options`: Optimize JDBC reading with partitioning
# MAGIC - `filters`: WHERE clause filters for source/target data
# MAGIC
# MAGIC See [Lakebridge Documentation](https://databrickslabs.github.io/lakebridge/docs/reconcile/recon_notebook/) for configuration examples.

# COMMAND ----------

# ============================================================================
# TABLE CONFIGURATIONS - UPDATE THESE WITH YOUR TABLES
# ============================================================================

table_recon = TableRecon(
    source_schema=database_config.source_schema,
    target_catalog=database_config.target_catalog,
    target_schema=database_config.target_schema,
    tables=[
        Table(
            source_name="edl_product_vw",  # Update source table name
            target_name="edl_product_vw",  # Update target table name
            join_columns= ["product_id"], # List of columns to join the source and target tables.
            drop_columns=["product_hash_key","product_hash","product_sk","source_file_date","system_modified_ts","last_viewed_ts","last_modifiedby_ts","last_modifiedby_id","last_referenced_ts","latest_ts"],
            transformations=[
                Transformation(
                    column_name="created_ts",
                    source="COALESCE(CAST(EXTRACT(EPOCH FROM created_ts) AS VARCHAR),'null_recon')",
                    target="COALESCE(CAST(unix_timestamp(created_ts) AS STRING),'null_recon')"
                ),
            ],
            filters = Filters(
                source="YEAR(created_ts) >= 2024",
                target="YEAR(created_ts) >= 2024"
            ),
            column_mapping=[],
            column_thresholds=[]
        ),
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Run Reconciliation Process
# MAGIC
# MAGIC **Description**: Executes the reconciliation comparing Dremio and Databricks data.
# MAGIC
# MAGIC **No input required** - Run as-is (uses configuration objects from Cell 4 and Cell 5).

# COMMAND ----------

ws = WorkspaceClient(product="lakebridge", product_version=__version__)

try:
  result = TriggerReconService.trigger_recon(
            ws = ws,
            spark = spark, # notebook spark session
            table_recon = table_recon, # previously created
            reconcile_config = reconcile_config # previously created
          )
  recon_id = result.recon_id
  print(f" Complete: {recon_id}")
  print("***************************")
except ReconciliationException as e:
  result = e.reconcile_output
  recon_id = result.recon_id
  print(f" Failed : {recon_id}")
  print(e)
  print("***************************")
except Exception as e:
  print(e.with_traceback)
  raise e
  print(f"Exception : {str(e)}")
  print("***************************")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Display Reconciliation Summary
# MAGIC
# MAGIC **Description**: Shows summary results for all reconciled tables.
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

# Initialize reporter with the correct catalog and schema
reporter = ReconcileReporter(spark, catalog=METADATA_CATALOG, schema=METADATA_SCHEMA)

# Get summary of all tables in the reconciliation run
summary_df = reporter.get_reconciliation_summary(recon_id)
table_id = summary_df.iloc[0]['recon_table_id']
print(f"Reconciliation Summary for {len(summary_df)} tables:")
display(summary_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Display Comparison Samples
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### I. Schema Comparison
# MAGIC
# MAGIC **Description**: Shows schema comparison results.
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

# get comparison samples
comparison_samples= reporter.get_sample_details(table_id, max_samples=100)

# Display schema comparison
if "schema" in comparison_samples.keys():
    display(comparison_samples["schema"])



# COMMAND ----------

# MAGIC %md
# MAGIC ### II. Mismatched Record Samples
# MAGIC
# MAGIC **Description**: Shows a sample of records that are present in both source/target data but have mismatched values.
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

mismatch_cols_str = summary_df.iloc[0]['mismatch_columns']
mismatch_cols = []
for c in mismatch_cols_str.split(','):
  mismatch_cols.extend([f"{c}_base",f"{c}_compare",f"{c}_match"])

# Display schema mismatch samples
if "mismatch" in comparison_samples.keys():
    try:
        display(comparison_samples["mismatch"][mismatch_cols])
    except:
        display(comparison_samples["mismatch"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### III. Missing in Source Samples
# MAGIC
# MAGIC **Description**: Shows a sample of records that appear in the target data (Databricks) but are missing from the source data (Dremio).
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

# Display schema missing_in_source samples
if "missing_in_source" in comparison_samples.keys():
    display(comparison_samples["missing_in_source"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### IV. Missing in Target Samples
# MAGIC
# MAGIC **Description**: Shows a sample of records that appear in the source data (Dremio) but are missing from the target data (Databricks).
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

# Display schema missing_in_source samples
if "missing_in_source" in comparison_samples.keys():
    display(comparison_samples["missing_in_source"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Export Reconciler Report
# MAGIC
# MAGIC **Description**: Generates comprehensive Excel report with all reconciliation details.
# MAGIC
# MAGIC **Optional User Inputs:**
# MAGIC - `OUTPUT_DIR`: - File name or folder where reconciler report will be saved if you do not wish to use the default value
# MAGIC

# COMMAND ----------

DEFAULT_OUTPUT_DIR = folders["reconciler_reports"]
OUTPUT_DIR = DEFAULT_OUTPUT_DIR
print(OUTPUT_DIR)
reporter.export_to_excel(recon_id, OUTPUT_DIR)

print(f"Reconciler report written to : {OUTPUT_DIR}")