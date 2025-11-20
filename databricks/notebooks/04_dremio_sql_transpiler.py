# Databricks notebook source
# MAGIC %md
# MAGIC # Dremio SQL Transpiler Notebook

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
# MAGIC ## 1. Load Configuration
# MAGIC
# MAGIC **Description**: Loads the saved configuration from setup notebook to get paths and settings.
# MAGIC
# MAGIC **Required User Input**:
# MAGIC - `VOLUME_PATH`: Volume path defined in the `01_lakebridge_dremio_setup` notebook.

# COMMAND ----------

import sys
import json
from datetime import datetime
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
# MAGIC **Description**: Configures Python path for Lakebridge transpiler imports.
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

# Import Dremio transpiler
from databricks.labs.lakebridge.transpiler.dremio_transpiler import DremioTranspiler

print("Dremio transpiler imported")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Set Import and Export Locations
# MAGIC
# MAGIC **Description:** Set the import and export directories for the Dremio SQL analyzer.
# MAGIC
# MAGIC **Optional User Input:**
# MAGIC   - `IMPORT_DIR`: Folder containing SQL queries to transpile.  Update if you do not want to use the `DEFAULT_IMPORT_DIR`.
# MAGIC   - `OUTPUT_DIR`: Folder where the transpiled SQL files will be saved.  Update if you do not want to use the `DEFAULT_OUTPUT_DIR`.
# MAGIC   - `ZIP_DIR`: Folder where a zipped file of the transpiled SQL file will be written.  Set to `None` to skip the zip file creation.

# COMMAND ----------

DEFAULT_IMPORT_DIR = str(Path(folders['source_sql']) / "dremio_queries")
DEFAULT_OUTPUT_DIR = str(Path(folders['transpiler_output'])/"transpiled_queries")
DEFAULT_ZIP_DIR = str(Path(folders['transpiler_output']))

IMPORT_DIR = DEFAULT_IMPORT_DIR
OUTPUT_DIR = DEFAULT_OUTPUT_DIR
ZIP_DIR = DEFAULT_ZIP_DIR   # Set to None to disable zipping

print(f"Default import directory: {DEFAULT_IMPORT_DIR}")
print(f"Default output directory: {DEFAULT_OUTPUT_DIR}")
print(f"Default zip directory: {DEFAULT_ZIP_DIR}")
print("\n")
print(f"Processing SQL files in: {IMPORT_DIR}")
print(f"Transpiled SQL files will be written to: {OUTPUT_DIR}")
print(f"Transpiled SQL files will be zipped to: {ZIP_DIR}" if ZIP_DIR else "Zipping disabled")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run Transpilation
# MAGIC
# MAGIC **Description**: Batch transpiles all Dremio SQL files to Databricks SQL dialect.
# MAGIC
# MAGIC **No input required** - Run as-is (automatically processes files from export step).

# COMMAND ----------

# Set paths
input_dir = Path(IMPORT_DIR)
output_dir = Path(OUTPUT_DIR)
zip_path = Path(ZIP_DIR) / f"transpiled_queries_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"

# Run batch transpilation
transpiler = DremioTranspiler(debug=False)
results = transpiler.batch_transpile(
    input_dir=input_dir,
    output_dir=output_dir,
    zip_filename=zip_path,
    pattern="*.sql"
)
print("Transpilation complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary
# MAGIC
# MAGIC **Description**: Provides summary of transpilation process.
# MAGIC
# MAGIC **No input required** - Run as-is (automatically processes files from export step).

# COMMAND ----------

# Print summary
success_count = sum(1 for r in results.values() if r.success)
failed_count = len(results) - success_count

print("\nTranspilation Summary:")
print(f"  Total files: {len(results)}")
print(f"  Successful: {success_count}")
print(f"  Failed: {failed_count}")

if failed_count > 0:
    print(f"\nError report saved to: {output_dir}/transpilation_errors.log")

print(f"\nTranspiled SQL files written to: {output_dir}")