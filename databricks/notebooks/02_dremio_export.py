# Databricks notebook source
# MAGIC %md
# MAGIC # Dremio View Export Notebook
# MAGIC
# MAGIC This notebook connects to Dremio and exports view definitions to SQL files.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Restart Python Environment (Recommended)
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
# MAGIC ## 1. Load Project Configuration
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

# Load saved configuration
VOLUME_PATH = "your-volume-storage-path"  # Storage volume path

config_file = Path(VOLUME_PATH) / "lakebridge-dremio" / "config" / "lakebridge_config.json"
with open(config_file, "r") as f:
    config = json.load(f)

LAKEBRIDGE_APP_PATH = config["lakebridge_path"]
folders = config["folders"]
SECRET_SCOPE = config["secret_scope"]

print(f"Configuration loaded from: {config_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configure Python Environment
# MAGIC
# MAGIC **Description**: Configures Python environment to import Dremio-Lakebridge modules.
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

# Import Dremio export utilities
from databricks.labs.lakebridge.helpers.dremio_utils import get_dremio_views, export_view_definitions

print("Dremio export utilities imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configure Dremio Source
# MAGIC
# MAGIC **Description**: Specifies which Dremio catalog and schema to export views from.
# MAGIC
# MAGIC **Required User Inputs**:
# MAGIC - `DREMIO_CATALOG`: The Dremio catalog containing your views
# MAGIC - `DREMIO_SCHEMA`: The Dremio schema/space containing your views
# MAGIC
# MAGIC **Optional Input**:
# MAGIC - `VIEW_NAME_PATTERN`: SQL LIKE pattern to filter views (e.g., 'edl_%' for views starting with 'edl_')

# COMMAND ----------

# Dremio source configuration
DREMIO_CATALOG = "DREMIO"  # Update this
DREMIO_SCHEMA = "commercial.crm"    # Update this
VIEW_NAME_PATTERN = "edl_%"         # Optional view name search pattern: 'edl_%', '%_vw', etc.

print(f"Source: {DREMIO_CATALOG}.{DREMIO_SCHEMA}")
if VIEW_NAME_PATTERN:
    print(f"Filter: {VIEW_NAME_PATTERN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Preview Available Views
# MAGIC
# MAGIC **Description**: Queries Dremio to list all available views matching the specified criteria.
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

# Get list of views
views_df = get_dremio_views(
    spark=spark,
    secret_scope=SECRET_SCOPE,
    catalog=DREMIO_CATALOG,
    schema=DREMIO_SCHEMA,
    view_pattern=VIEW_NAME_PATTERN
)

# Display views
display(views_df.select("TABLE_NAME").orderBy("TABLE_NAME"))
print(f"\nTotal views found: {views_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Set Export Directory
# MAGIC
# MAGIC **Description:** Set the directorys where the exported view definitions, and optional zip file, will be saved.
# MAGIC
# MAGIC **Optional User Input:**
# MAGIC   - `OUTPUT_DIR`: Folder where exported view definitions will be save.  Update if you do not wish to use the `DEFAULT_OUTPUT_DIR`.
# MAGIC   - `ZIP_DIR`: Folder where an optional zip file of exported view SQL files will be saved.  Update if you do not want to use the `DEFAULT_ZIP_DIR`.

# COMMAND ----------

DEFAULT_OUTPUT_DIR = f"{folders['source_sql']}/dremio_queries"
DEFAULT_ZIP_DIR = f"{folders['source_sql']}"

OUTPUT_DIR = DEFAULT_OUTPUT_DIR
ZIP_DIR = DEFAULT_ZIP_DIR    # Set to None if you don't want to save a zip file

print(f"Default output directory: {DEFAULT_OUTPUT_DIR}")
print(f"Default zip file directory: {DEFAULT_ZIP_DIR}")
print("\n")
print(f"Saving exported views in: {OUTPUT_DIR}")
print(f"Saving zip file of views to : {ZIP_DIR}" if ZIP_DIR else "No zip file saved")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Export View Definitions
# MAGIC
# MAGIC **Description**: Exports all view SQL definitions to individual files and optionally creates a zip archive.
# MAGIC
# MAGIC **No input required** - Run as-is (uses configuration from previous cells).

# COMMAND ----------

# Configure output paths
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
query_out_path = OUTPUT_DIR
zip_out_path = f"{ZIP_DIR}/{DREMIO_SCHEMA}_{timestamp}.zip" if ZIP_DIR else None

# Export views
export_result = export_view_definitions(
    spark=spark,
    secret_scope=SECRET_SCOPE,
    catalog=DREMIO_CATALOG,
    schema=DREMIO_SCHEMA,
    query_out_path=query_out_path,
    view_pattern=VIEW_NAME_PATTERN,
    zip_out_path=zip_out_path  # Set to None to skip zip
)

print("Export Summary:")
print(f"  Views exported: {export_result['view_count']}")
print(f"  Output directory: {export_result['output_directory']}")
if export_result['zip_file']:
    print(f"  Archive: {export_result['zip_file']}")