"""
Dremio specific helper functions
"""

import os
import re
import zipfile
import logging
import tempfile
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Callable
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)


def safe_filename(name: str) -> str:
    """
    Convert a string to a safe filename.

    Args:
        name: Original name

    Returns:
        Safe filename with only alphanumeric, dash, underscore, dot
    """
    return re.sub(r"[^a-zA-Z0-9\-_\.]", "_", name)


def create_zip_archive(
    files: List[Union[str, Path]],
    output_path: Union[str, Path],
    archive_name_prefix: Optional[str] = None,
    use_relative_paths: bool = True
) -> Optional[str]:
    """
    Create a zip archive from a list of files.

    Args:
        files: List of file paths to include in the archive
        output_path: Path for the output zip file
        archive_name_prefix: Optional prefix for files inside the archive
        use_relative_paths: If True, use only filenames in archive; if False, preserve directory structure

    Returns:
        Path to created zip file, or None if creation failed
    """
    if not files:
        logger.warning("No files provided for zip archive")
        return None

    try:
        # Use temporary directory for safe creation
        temp_dir = tempfile.mkdtemp()

        try:
            # Create zip in temp directory first
            temp_zip_path = Path(temp_dir) / "temp_archive.zip"

            logger.info(f"Creating zip archive with {len(files)} files")

            with zipfile.ZipFile(str(temp_zip_path), 'w', zipfile.ZIP_DEFLATED) as zf:
                for file_path in files:
                    file_path = Path(file_path)

                    if not file_path.exists():
                        logger.warning(f"File not found, skipping: {file_path}")
                        continue

                    # Determine archive name
                    if use_relative_paths:
                        arcname = file_path.name
                    else:
                        # Preserve relative directory structure
                        arcname = str(file_path)

                    # Add prefix if provided
                    if archive_name_prefix:
                        arcname = f"{archive_name_prefix}/{arcname}"

                    zf.write(str(file_path), arcname)
                    logger.debug(f"Added to archive: {arcname}")

            # Move from temp to final location
            final_zip_path = Path(output_path)
            final_zip_path.parent.mkdir(parents=True, exist_ok=True)

            shutil.move(str(temp_zip_path), str(final_zip_path))

            # Verify zip was created
            if final_zip_path.exists():
                zip_size = final_zip_path.stat().st_size
                logger.info(f"Archive created: {final_zip_path} ({zip_size:,} bytes)")
                return str(final_zip_path)
            else:
                logger.error(f"Zip file was not created at {final_zip_path}")
                return None

        finally:
            # Clean up temp directory
            if Path(temp_dir).exists():
                shutil.rmtree(temp_dir)

    except Exception as e:
        logger.error(f"Failed to create zip archive: {e}")
        return None


def get_dremio_views(
    spark,
    secret_scope: str,
    catalog: str,
    schema: str,
    view_pattern: Optional[str] = None
):
    """
    Get list of Dremio views and their definitions.

    Args:
        spark: SparkSession
        secret_scope: Databricks secret scope containing Dremio credentials
        catalog: Dremio catalog name
        schema: Dremio schema name
        view_pattern: Optional SQL LIKE pattern to filter views (e.g., 'vw_%', '%_view', '%customer%')

    Returns:
        DataFrame with view information and definitions
    """
    # Get credentials from secret scope
    from pyspark.dbutils import DBUtils
    dbutils = DBUtils(spark)

    host = dbutils.secrets.get(scope=secret_scope, key="host")
    port = dbutils.secrets.get(scope=secret_scope, key="port")
    username = dbutils.secrets.get(scope=secret_scope, key="username")
    password = dbutils.secrets.get(scope=secret_scope, key="password")

    # Build JDBC connection
    jdbc_url = f"jdbc:dremio:direct={host}:{port}"
    connection_props = {
        "driver": "com.dremio.jdbc.Driver",
        "user": username,
        "password": password
    }
    # Build the query
    view_filter = f"AND v.TABLE_NAME LIKE '{view_pattern}'" if view_pattern else ""

    views_query = f"""
    (
        SELECT
            v.TABLE_CATALOG,
            v.TABLE_SCHEMA,
            v.TABLE_NAME,
            v.VIEW_DEFINITION
        FROM INFORMATION_SCHEMA.VIEWS v
        WHERE
            v.TABLE_CATALOG = '{catalog}'
            AND v.TABLE_SCHEMA = '{schema}'
            {view_filter}
        ORDER BY v.TABLE_NAME
    ) t
    """

    logger.info(f"Fetching views from {catalog}.{schema}")
    if view_pattern:
        logger.info(f"Using filter pattern: {view_pattern}")

    # Pull views via JDBC
    views_df = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", views_query)
        .options(**connection_props)
        .load()
    )

    return views_df


def export_view_definitions(
    spark,
    secret_scope: str,
    catalog: str,
    schema: str,
    query_out_path: str,
    view_pattern: Optional[str] = None,
    zip_out_path: Optional[str] = None
) -> Dict[str, any]:
    """
    Export Dremio view definitions to SQL files.

    Args:
        spark: SparkSession
        secret_scope: Databricks secret scope containing Dremio credentials
        catalog: Dremio catalog name
        schema: Dremio schema name
        query_out_path: Directory path for SQL files output
        view_pattern: Optional SQL LIKE pattern to filter views (e.g., 'vw_%', '%_view', '%customer%')
        zip_out_path: Optional path for zip file output (None = no zip)

    Returns:
        Dictionary with export results
    """
    # Get views
    views_df = get_dremio_views(spark, secret_scope, catalog, schema, view_pattern)

    # Create output directory
    queries_dir = Path(query_out_path)
    queries_dir.mkdir(parents=True, exist_ok=True)

    # Collect and export views
    rows = views_df.collect()
    exported_files = []

    logger.info(f"Exporting {len(rows)} view definitions to {queries_dir}")

    for row in rows:
        catalog_name = row['TABLE_CATALOG']
        schema_name = row['TABLE_SCHEMA']
        view_name = row['TABLE_NAME']
        view_sql = row['VIEW_DEFINITION']

        # Create filename
        filename = f"{safe_filename(view_name)}.sql"
        filepath = queries_dir / filename

        # Write SQL file
        with open(filepath, 'w') as f:
            f.write(view_sql)
            if not view_sql.endswith('\n'):
                f.write('\n')

        exported_files.append(str(filepath))
        logger.debug(f"Exported: {filename}")

    # Create zip if requested
    if zip_out_path and exported_files:
        zip_result = create_zip_archive(
            files=exported_files,
            output_path=zip_out_path,
            use_relative_paths=True
            )
        zip_out_path = zip_result

    return {
        "view_count": len(rows),
        "exported_files": exported_files,
        "output_directory": str(queries_dir),
        "zip_file": str(zip_out_path) if zip_out_path else None
    }


def write_excel_file(
    excel_writer_func: Callable[[Any], None],
    output_path: Union[str, Path],
    auto_adjust_columns: bool = True,
    max_column_width: int = 50
) -> str:
    """
    Write an Excel file safely, using a temp file approach that works in Unity Catalog Volumes.

    This function handles the common pattern of:
    1. Creating Excel in a temp location
    2. Writing data using pd.ExcelWriter
    3. Auto-adjusting column widths
    4. Moving/copying to final destination
    5. Cleaning up temp files

    Args:
        excel_writer_func: A function that takes a pd.ExcelWriter object and writes data to it.
                          This function should handle all sheet creation and data writing.
        output_path: Final destination path for the Excel file
        auto_adjust_columns: Whether to auto-adjust column widths for better readability
        max_column_width: Maximum column width when auto-adjusting

    Returns:
        Path to the created Excel file

    Example:
        def write_data(writer):
            df1.to_excel(writer, sheet_name='Summary', index=False)
            df2.to_excel(writer, sheet_name='Details', index=False)

        output_file = write_excel_file(write_data, '/path/to/output.xlsx')
    """
    output_path = Path(output_path)

    # Ensure output path has .xlsx extension
    if not output_path.suffix:
        output_path = output_path.with_suffix('.xlsx')

    # Create temp directory for safe file creation
    temp_dir = tempfile.mkdtemp()
    temp_path = Path(temp_dir) / output_path.name

    try:
        # Create Excel file in temp location
        with pd.ExcelWriter(temp_path, engine='openpyxl', mode='w') as writer:
            # Call the user's function to write data
            excel_writer_func(writer)

            # Auto-adjust column widths if requested
            if auto_adjust_columns and writer.sheets:
                for sheet_name in writer.sheets:
                    worksheet = writer.sheets[sheet_name]
                    for column in worksheet.columns:
                        max_length = 0
                        column_letter = column[0].column_letter
                        for cell in column:
                            try:
                                cell_value = str(cell.value) if cell.value is not None else ""
                                if len(cell_value) > max_length:
                                    max_length = len(cell_value)
                            except:
                                pass
                        adjusted_width = min(max_length + 2, max_column_width)
                        worksheet.column_dimensions[column_letter].width = adjusted_width

        # Ensure parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Move temp file to final destination
        # Using shutil.move which works across filesystems
        shutil.move(str(temp_path), str(output_path))

        logger.info(f"Excel file created: {output_path}")
        return str(output_path)

    except PermissionError as e:
        logger.error(f"Permission denied writing to {output_path}. File may be open in Excel.")
        raise
    except Exception as e:
        logger.error(f"Error creating Excel file: {e}")
        raise
    finally:
        # Clean up temp directory
        shutil.rmtree(temp_dir, ignore_errors=True)