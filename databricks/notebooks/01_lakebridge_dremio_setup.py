# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebridge-Dremio Setup Notebook
# MAGIC
# MAGIC This notebook sets up the environment for running Lakebridge workflows with Dremio:
# MAGIC - Installs required Python packages
# MAGIC - Creates folder structure in storage volume
# MAGIC - Configures Dremio credentials in secrets
# MAGIC
# MAGIC ## Prerequisites
# MAGIC   
# MAGIC Before running this setup notebook, ensure you have:
# MAGIC
# MAGIC ### Databricks Environment
# MAGIC - **Workspace access** with ability to create notebooks and run clusters
# MAGIC - **Git Folder** connected to EDP repo
# MAGIC - **Unity Catalog schema** with CREATE TABLE and DROP TABLE permissions
# MAGIC - **Volume** with read/write permissions for storing drivers, configs, and output files
# MAGIC - **Personal compute cluster**
# MAGIC
# MAGIC ### Access and Permissions
# MAGIC - **Enterprise-Data-Platform** repo containing the Lakebridge-Dremio appication code
# MAGIC - **Read access** to source Dremio tables/views
# MAGIC - **Read access** to target Unity Catalog tables
# MAGIC - **Secret scope permissions** to create/manage secrets in workspace
# MAGIC

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
# MAGIC ## 1. Configuration
# MAGIC
# MAGIC **Description**: Defines project paths, volume locations, and Dremio connection parameters.
# MAGIC
# MAGIC **Required User Inputs**:
# MAGIC - `LAKEBRIDGE_APP_PATH`: Path to your Lakebridge-Dremio application (e.g., "/Workspace/Users/your.email@company.com/Enterprise-Data-Platform/projects/lakebridge-dremio")
# MAGIC - `PROJECT_CATALOG`: Databricks catalog that contains the project schema 
# MAGIC - `PROJECT_SCHEMA`: Project schema where reconciler metadata tables will be written
# MAGIC - `VOLUME_PATH`: Unity Catalog volume path for storing artifacts (e.g., "/Volumes/main/default/lakebridge_volume")
# MAGIC - `DREMIO_HOST`: Your Dremio cluster hostname
# MAGIC - `DREMIO_USERNAME`: Your Dremio username for authentication
# MAGIC - `DREMIO_PASSWORD`: Your Dremio password for authentication
# MAGIC
# MAGIC **Optional Inputs**:
# MAGIC - `DREMIO_PORT`: Dremio port (defaults to "31010")
# MAGIC - `SECRET_SCOPE`: Secret scope name (defaults to "lakebridge-credentials"). Set a unique name if working in a shared workspace.

# COMMAND ----------

# Project configuration
LAKEBRIDGE_APP_PATH = "/Workspace/Users/user@corteva.com/Enterprise-Data-Platform/projects/lakebridge-dremio"  # Path to lakebridge-dremio app

# Project schema and volume
PROJECT_CATALOG = "name-of-project-catalog"
PROJECT_SCHEMA = "name-of-project-schema"
VOLUME_PATH = "volume-storage-path"  # Storage volume path

# Dremio connection parameters
DREMIO_HOST = "edp-dremio-prod.corteva.com"
DREMIO_PORT = "31010"
DREMIO_USERNAME = "user@corteva.com"
DREMIO_PASSWORD = "dremio-access-token"  # Remove password after creating secret

# Secret scope name
SECRET_SCOPE = "lakebridge-credentials" # Secret scope name 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Connect to Lakebridge-Dremio App
# MAGIC
# MAGIC **Description**: Adds Lakebridge-Dremio app to Python path and verifies the module can be imported.
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

import os
import sys
import importlib.util

# Setup paths
databricks_path = f"{LAKEBRIDGE_APP_PATH}/databricks"

# Verify paths exist
assert os.path.isdir(LAKEBRIDGE_APP_PATH), f"App path not found: {LAKEBRIDGE_APP_PATH}"
assert os.path.isdir(databricks_path), f"Databricks folder not found: {databricks_path}"
assert os.path.isdir(f"{databricks_path}/labs/lakebridge"),f"Lakebridge not found in: {databricks_path}"

# Add to sys.path
if LAKEBRIDGE_APP_PATH not in sys.path:
    sys.path.insert(0, LAKEBRIDGE_APP_PATH)

# Fix databricks namespace
import databricks
if databricks_path not in databricks.__path__:
    databricks.__path__.insert(0, databricks_path) 

# Verify import works
found = importlib.util.find_spec("databricks.labs.lakebridge")
assert found, "Failed to locate databricks.labs.lakebridge module"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Import Setup Utils
# MAGIC
# MAGIC **Description**: Imports utility functions used for new project setup.
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

# Now import the setup utilities
from databricks.dremio_setup_utils import(
    create_folder_structure,
    setup_dremio_secret,
    copy_drivers,
    copy_requirements,
    save_config,
    load_config
)
print("Setup utilities imported successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create Folder Structure
# MAGIC
# MAGIC **Description**: Creates organized folder structure in Unity Catalog volume for storing drivers, SQL files, and reports.
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

# Create organized folder structure in volume
folders = create_folder_structure(VOLUME_PATH)

print("\nCreated folders:")
for name, path in folders.items():
    print(f"  {name}: {path}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Copy Requirements and Dremio JDBC Driver to Volume
# MAGIC
# MAGIC **Description**: Copies the following files to project volume storage:
# MAGIC - requirements.txt - file containing list of required python packages
# MAGIC - JDBC driver JAR files from project to volume for Dremio connectivity.
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

requirements_path = copy_requirements(LAKEBRIDGE_APP_PATH, VOLUME_PATH)
if requirements_path:
    print(f"Requirements file copied to: {requirements_path}\n")

driver_paths = copy_drivers(LAKEBRIDGE_APP_PATH, VOLUME_PATH)
if driver_paths:
    print(f"Copied {len(driver_paths)} drivers to {VOLUME_PATH}/Drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Setup Dremio Credentials
# MAGIC
# MAGIC **Description**: Stores Dremio connection credentials securely in Databricks secret scope.
# MAGIC
# MAGIC **No input required** - Run as-is (uses values from Cell 1).

# COMMAND ----------

# Store Dremio credentials in secret scope
setup_dremio_secret(
    scope_name=SECRET_SCOPE,
    host=DREMIO_HOST,
    port=DREMIO_PORT,
    username=DREMIO_USERNAME,
    password=DREMIO_PASSWORD
)

if any(s.name == SECRET_SCOPE for s in dbutils.secrets.listScopes()):
    print(f"{SECRET_SCOPE} created.  Remove password from notebook.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Save Configuration
# MAGIC
# MAGIC **Description**: Saves all configuration settings to JSON file for use in subsequent notebooks.
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

config = {
    "lakebridge_path": LAKEBRIDGE_APP_PATH,
    "project_catalog": PROJECT_CATALOG,
    "project_schema": PROJECT_SCHEMA,
    "volume_path": VOLUME_PATH,
    "folders": folders,
    "secret_scope": SECRET_SCOPE,
    "dremio": {
        "host": DREMIO_HOST,
        "port": DREMIO_PORT,
    }
}
save_config(VOLUME_PATH, config)
print("\nConfiguration saved.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Setup Summary
# MAGIC
# MAGIC **Description**: Displays setup summary.
# MAGIC
# MAGIC **No input required** - Run as-is.

# COMMAND ----------

# Verify Setup
print("Setup Complete!")
print(f"Lakebridge app path: {LAKEBRIDGE_APP_PATH}")
print(f"Project Catalog: {PROJECT_CATALOG}")
print(f"Project Schema: {PROJECT_SCHEMA}")
print(f"Volume base path: {VOLUME_PATH}")
print(f"Secret scope: {SECRET_SCOPE}")
print(f"Copied JDBC drivers: {driver_paths}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Add Requirements file as Cluster Library (Manual Steps)
# MAGIC
# MAGIC Before running the other notebooks, the required python libraries must be installed on your cluster. 
# MAGIC
# MAGIC 1. **Go to Compute:**
# MAGIC     - Go to Compute in Databricks workspace
# MAGIC     - Select your personal cluster
# MAGIC 2. **Go to Libraries tab:**
# MAGIC     - Click on the "Libraries" tab in your cluster configuration
# MAGIC 3. **Install New -> `requirements.txt`:**
# MAGIC     - Click "Install New"
# MAGIC     - Select "Volumes" as the source
# MAGIC     - Navigate to the `requirements.txt` saved in the Requirements folder
# MAGIC     - Click "Install"
# MAGIC 4. **Restart Cluster (If Running):**
# MAGIC     - Restart your personal cluster

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Install Dremio JDBC Driver on Personal Cluster (Manual steps):
# MAGIC
# MAGIC 1. **Navigate to Compute:**
# MAGIC     - Go to Compute in the Databricks workspace
# MAGIC     - Select your personal cluster
# MAGIC 2. **Go to Libraries tab:**
# MAGIC     - Click on the "Libraries" tab in your cluster configuration
# MAGIC 3. **Install New -> JAR:**
# MAGIC     - Click "Install New"
# MAGIC     - Select "JAR" as the library type
# MAGIC     - Choose "Volumes" as the source
# MAGIC     - Navigate to the Dremio driver JAR saved in the Drivers folder
# MAGIC     - Click "Install"
# MAGIC 4. **Restart Cluster (If Running):**
# MAGIC     - Restart your personal cluster