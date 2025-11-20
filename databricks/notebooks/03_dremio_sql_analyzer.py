# Databricks notebook source
# MAGIC %md
# MAGIC # Dremio SQL Analyzer Notebook
# MAGIC
# MAGIC This notebook analyzes the exported Dremio SQL queries using the specialized Dremio analyzer.

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

# Load saved configuration
VOLUME_PATH = "your-volume-storage-path"  # Storage volume path

config_file = Path(VOLUME_PATH) / "lakebridge-dremio" / "config" / "lakebridge_config.json"
with open(config_file, "r") as f:
    config = json.load(f)

LAKEBRIDGE_APP_PATH = config["lakebridge_path"]
folders = config["folders"]

print(f"Configuration loaded from: {config_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configure Python Environment
# MAGIC
# MAGIC **Description**: Configures Python environment for Lakebridge module imports.
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

# Set path to Lakebridge-Dremio App
databricks_module_path = f"{LAKEBRIDGE_APP_PATH}/databricks"

if LAKEBRIDGE_APP_PATH not in sys.path:
    sys.path.insert(0, LAKEBRIDGE_APP_PATH)

import databricks
if databricks_module_path not in databricks.__path__:
    databricks.__path__.insert(0, databricks_module_path)

# Import Dremio analyzer
from databricks.labs.lakebridge.analyzer.dremio_batch_analyzer import DremioBatchAnalyzer

print("Dremio batch analyzer imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Set Import and Export Directories
# MAGIC
# MAGIC **Description:** Set the import and export directories for the Dremio SQL analyzer.
# MAGIC
# MAGIC **Optional User Input:**
# MAGIC   - `IMPORT_DIR`: Folder containing SQL queries to analyze.  Update if you do not want to use the `DEFAULT_IMPORT_DIR`.
# MAGIC   - `OUTPUT_DIR`: Folder where the analysis report will be saved.  Update if you do not want to use the `DEFAULT_OUTPUT_DIR`.

# COMMAND ----------

DEFAULT_IMPORT_DIR = str(Path(folders['source_sql']) / "dremio_queries")
DEFAULT_OUTPUT_DIR = str(Path(folders['analyzer_output']))

IMPORT_DIR = DEFAULT_IMPORT_DIR
OUTPUT_DIR = DEFAULT_OUTPUT_DIR

print(f"Default import directory: {DEFAULT_IMPORT_DIR}")
print(f"Default output directory: {DEFAULT_OUTPUT_DIR}")
print("\n")
print(f"Processing SQL files in: {IMPORT_DIR}")
print(f"Analysis report will be written to: {OUTPUT_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run SQL Analysis
# MAGIC
# MAGIC **Description**: Analyzes all exported Dremio SQL files and generates compatibility report in Excel format.
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

from datetime import datetime

# Create analyzer and analyze directory
query_dir = IMPORT_DIR
out_file = f"{OUTPUT_DIR}/dremio_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

analyzer = DremioBatchAnalyzer(debug=False)
analyzer.run(query_dir, out_file)

print(f"Analysis complete! Report written to: {out_file}")