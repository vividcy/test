"""
Reconciliation reporting utilities for generating reports from reconciliation metadata.

This module provides functions to:
- Extract reconciliation results from Unity Catalog metadata tables
- Generate summary and detailed reports as pandas DataFrames
- Export reports to Excel files with multiple tabs
"""

import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Optional
import pandas as pd
from pyspark.sql import SparkSession
import logging
from databricks.labs.lakebridge.helpers.dremio_utils import write_excel_file

logger = logging.getLogger(__name__)


class ReconcileReporter:
    """Generate reports from reconciliation metadata tables."""

    def __init__(
        self,
        spark: SparkSession,
        catalog: str = "remorph",
        schema: str = "reconcile"
    ):
        """
        Initialize the reporter with Spark session and metadata location.

        Args:
            spark: SparkSession for querying metadata tables
            catalog: Catalog containing reconciliation metadata tables
            schema: Schema containing reconciliation metadata tables
        """
        self.spark = spark
        self.catalog = catalog
        self.schema = schema

    def get_reconciliation_summary(self, recon_id: str) -> pd.DataFrame:
        """
        Get summary of all tables in a reconciliation run.

        Args:
            recon_id: Reconciliation run identifier

        Returns:
            DataFrame with summary of all tables reconciled
        """
        query = f"""
        SELECT
            main.recon_table_id,
            main.source_type,
            main.report_type,
            IF(
                ISNULL(main.source_table.`catalog`),
                CONCAT_WS('.', main.source_table.`schema`, main.source_table.table_name),
                CONCAT_WS('.', main.source_table.`catalog`, main.source_table.`schema`, main.source_table.table_name)
            ) AS source_table,
            CONCAT(main.target_table.`catalog`, '.', main.target_table.`schema`, '.', main.target_table.table_name) AS target_table,
            metrics.record_count.source as source_count,
            metrics.record_count.target as target_count,
            metrics.recon_metrics.row_comparison.missing_in_source as missing_in_source,
            metrics.recon_metrics.row_comparison.missing_in_target as missing_in_target,
            metrics.recon_metrics.column_comparison.absolute_mismatch as column_mismatches,
            metrics.recon_metrics.column_comparison.threshold_mismatch as threshold_mismatches,
            metrics.recon_metrics.column_comparison.mismatch_columns as mismatch_columns,
            metrics.recon_metrics.schema_comparison as schema_match,
            metrics.run_metrics.status as status,
            metrics.run_metrics.exception_message as exception,
            main.start_ts,
            main.end_ts,
            CAST((unix_timestamp(main.end_ts) - unix_timestamp(main.start_ts)) AS INT) as duration_seconds
        FROM {self.catalog}.{self.schema}.main AS main
        LEFT JOIN {self.catalog}.{self.schema}.metrics AS metrics
            ON main.recon_table_id = metrics.recon_table_id
        WHERE main.recon_id = '{recon_id}'
        ORDER BY main.recon_table_id
        """

        try:
            return self.spark.sql(query).toPandas()
        except Exception as e:
            logger.warning(f"Arrow optimization failed, retrying without Arrow: {e}")
            # Fallback: disable Arrow and retry
            original_arrow = self.spark.conf.get("spark.sql.execution.arrow.pyspark.enabled", "true")
            try:
                self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
                return self.spark.sql(query).toPandas()
            finally:
                self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", original_arrow)

    def get_table_metrics(self, recon_table_id: int) -> pd.DataFrame:
        """
        Get detailed metrics for a specific table.

        Args:
            recon_table_id: Table reconciliation identifier

        Returns:
            DataFrame with detailed metrics for the table
        """
        query = f"""
        SELECT
            metrics.recon_table_id,
            metrics.record_count.source as source_records,
            metrics.record_count.target as target_records,
            metrics.recon_metrics.row_comparison.missing_in_source as missing_in_source,
            metrics.recon_metrics.row_comparison.missing_in_target as missing_in_target,
            metrics.recon_metrics.column_comparison.absolute_mismatch as column_mismatches,
            metrics.recon_metrics.column_comparison.threshold_mismatch as threshold_mismatches,
            metrics.recon_metrics.column_comparison.mismatch_columns as mismatch_columns,
            metrics.recon_metrics.schema_comparison as schema_match,
            metrics.run_metrics.status as status,
            metrics.run_metrics.exception_message as exception
        FROM {self.catalog}.{self.schema}.metrics metrics
        WHERE metrics.recon_table_id = {recon_table_id}
        """

        return self.spark.sql(query).toPandas()

    def get_sample_details(
        self,
        recon_table_id: int,
        detail_type: str = None,
        max_samples: int = 100
    ) -> Dict[str, pd.DataFrame]:
        """
        Get sample records for mismatches and missing records.

        Args:
            recon_table_id: Table reconciliation identifier
            detail_type: Type of details ('mismatch', 'missing_in_source', 'missing_in_target', or None for all)
            max_samples: Maximum number of samples to return per type

        Returns:
            Dict with recon_type as keys and DataFrames as values
        """
        type_filter = f"AND details.recon_type = '{detail_type}'" if detail_type else ""

        query = f"""
        SELECT
            details.recon_type,
            details.status,
            size(details.data) as total_samples,
            slice(details.data, 1, {max_samples}) as sample_records
        FROM {self.catalog}.{self.schema}.details details
        WHERE details.recon_table_id = {recon_table_id}
        {type_filter}
        """

        # Disable Arrow optimization for this query due to complex nested structures 
        original_arrow_enabled = self.spark.conf.get("spark.sql.execution.arrow.pyspark.enabled", "true") 
        try: # Temporarily disable Arrow optimization 
            self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false") 
            df = self.spark.sql(query).toPandas() 
        finally: # Restore original setting 
            self.spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", original_arrow_enabled)

        # Organize samples by recon_type
        samples_by_type = {}

        if not df.empty and 'sample_records' in df.columns:
            for _, row in df.iterrows():
                recon_type = row['recon_type']
                if row['sample_records'] and row['sample_records'] is not None:
                    try:
                        # Expand records for this type
                        expanded_rows = []
                        for record in row['sample_records']:
                            if record is not None:
                                expanded_rows.append(record)

                        if expanded_rows:
                            df = pd.DataFrame(expanded_rows)
                            df = self._sort_sample_columns(df, recon_type)
                            samples_by_type[recon_type] = df
                    except Exception as e:
                        logger.warning(f"Error expanding records for recon_type '{recon_type}': {e}")

        return samples_by_type
    
    def _sort_sample_columns(self, df: pd.DataFrame, recon_type: str) -> pd.DataFrame:
        """
        Sort columns for better readability.

        For schema type: preserve orignal column order
        For mismatch type: group related columns (base, compare, match) together
        For other types: sort alphabetically
        """
        if df.empty:
            return df

        columns = df.columns.tolist()

        # Separate metadata columns that should come first
        metadata_columns = []
        data_columns = []

        for col in columns:
            if col in ["table_id", "source_table", "target_table"]:
                metadata_columns.append(col)
            else:
                data_columns.append(col)

        if recon_type == 'mismatch':
            # For mismatch, group columns by base name (everything before last underscore)
            # Order should be: column_base, column_compare, column_match
            grouped_columns = {}
            other_columns = []

            for col in data_columns:
                if '_' in col and col.split('_')[-1] in ['base', 'compare', 'match']:
                    # Get base name by removing last underscore part
                    base_name = '_'.join(col.split('_')[:-1])
                    suffix = col.split('_')[-1]

                    if base_name not in grouped_columns:
                        grouped_columns[base_name] = {}
                    grouped_columns[base_name][suffix] = col
                else:
                    other_columns.append(col)

            # Build ordered column list
            ordered_data_columns = []

            # Sort group names alphabetically
            for base_name in sorted(grouped_columns.keys()):
                group = grouped_columns[base_name]
                # Order within group: _base, _compare, _match
                for suffix in ['base', 'compare', 'match']:
                    if suffix in group:
                        ordered_data_columns.append(group[suffix])

            # Add other columns at the end, sorted
            ordered_data_columns.extend(sorted(other_columns))

        elif recon_type == 'schema':
            # For schema comparison, preserve the intended column order
            schema_column_order = [
                'source_column', 
                'source_datatype', 
                'databricks_column', 
                'databricks_datatype', 
                'is_valid'
            ]
            ordered_data_columns = [c for c in schema_column_order if c in data_columns]
            ordered_data_columns += [c for c in data_columns if c not in ordered_data_columns]

        else:
            # For other types, just sort alphabetically
            ordered_data_columns = sorted(data_columns)

        # Combine metadata columns with ordered data columns
        final_columns = metadata_columns + ordered_data_columns

        # Reorder dataframe columns
        return df[final_columns]

    def get_all_sample_details(
        self,
        recon_id: str,
        max_samples_per_table: int = 100
    ) -> Dict[str, pd.DataFrame]:
        """
        Get sample records for all tables in a reconciliation run, organized by recon_type.

        For each recon_type, this extracts samples from all tables and concatenates them
        into a single DataFrame with added table identifier columns.

        Note: Different recon_types have different column structures:
        - mismatch: Contains columns from both source and target showing differences
        - missing_in_source: Contains only target table columns
        - missing_in_target: Contains only source table columns

        Args:
            recon_id: Reconciliation run identifier
            max_samples_per_table: Maximum samples per table

        Returns:
            Dict with recon_type as keys and single concatenated DataFrame per type
        """
        # Get all table IDs for this reconciliation
        table_ids = self.get_table_ids(recon_id)

        # Initialize dict to collect DataFrames by type
        # Will be populated dynamically based on what's in the data
        all_samples_by_type = {}

        # Iterate through each table and collect samples
        for table_id, source_table, target_table in table_ids:
            # Get samples for this table organized by type
            table_samples = self.get_sample_details(table_id, max_samples=max_samples_per_table)

            # Process each recon_type for this table
            for recon_type, df in table_samples.items():
                if not df.empty:
                    # Add table identification columns at the beginning
                    df.insert(0, 'table_id', table_id)
                    df.insert(1, 'source_table', source_table)
                    df.insert(2, 'target_table', target_table)

                    # Initialize list for this type if not exists
                    if recon_type not in all_samples_by_type:
                        all_samples_by_type[recon_type] = []

                    # Append to the list for this recon_type
                    all_samples_by_type[recon_type].append(df)

        # Concatenate all DataFrames for each recon_type
        result = {}
        for recon_type, df_list in all_samples_by_type.items():
            if df_list:
                # Concatenate all DataFrames for this type
                # They share the same columns within each type
                df = pd.concat(df_list, ignore_index=True)
                df = self._sort_sample_columns(df, recon_type)
                result[recon_type] = df

        return result

    def get_statistics_summary(self, recon_id: str) -> pd.DataFrame:
        """
        Generate statistical summary of the reconciliation.

        Args:
            recon_id: Reconciliation run identifier

        Returns:
            DataFrame with statistical summary
        """
        summary_df = self.get_reconciliation_summary(recon_id)

        if summary_df.empty:
            return pd.DataFrame()

        # Calculate statistics
        stats = []

        # Overall statistics
        total_tables = len(summary_df)
        tables_with_issues = len(summary_df[
            (summary_df['missing_in_source'] > 0) |
            (summary_df['missing_in_target'] > 0) |
            (summary_df['column_mismatches'] > 0)
        ])

        total_source_records = summary_df['source_count'].sum()
        total_target_records = summary_df['target_count'].sum()
        total_mismatched = summary_df['column_mismatches'].sum()
        total_missing_source = summary_df['missing_in_source'].sum()
        total_missing_target = summary_df['missing_in_target'].sum()

        # Create statistics rows
        stats.append({
            'Metric': 'Total Tables',
            'Value': total_tables,
        })
        stats.append({
            'Metric': 'Tables with Issues',
            'Value': tables_with_issues,
        })
        stats.append({
            'Metric': 'Total Source Records',
            'Value': total_source_records,
        })
        stats.append({
            'Metric': 'Total Target Records',
            'Value': total_target_records,
        })
        stats.append({
            'Metric': 'Records with Column Mismatches',
            'Value': total_mismatched,
        })
        stats.append({
            'Metric': 'Records Missing in Source',
            'Value': total_missing_source,
        })
        stats.append({
            'Metric': 'Records Missing in Target',
            'Value': total_missing_target,
        })

        return pd.DataFrame(stats)
    
    def _create_field_definitions_dataframe(self) -> pd.DataFrame:
        """Create documentation for reconciliation report fields"""

        definitions = [
            # --- Summary ---
            {
                'Sheet': 'Summary',
                'Term': 'recon_table_id',
                'Definition': 'Unique identifier for a specific table within a reconciliation run'
            },
            {
                'Sheet': 'Summary',
                'Term': 'source_type',
                'Definition': 'Type of source system (e.g., Dremio)'
            },
            {
                'Sheet': 'Summary',
                'Term': 'report_type',
                'Definition': 'Type of reconciliation: data (full data comparison), schema (structure only), all (both data and schema)'
            },
            {
                'Sheet': 'Summary',
                'Term': 'source_table',
                'Definition': 'Full qualified name of the source table'
            },
            {
                'Sheet': 'Summary',
                'Term': 'target_table',
                'Definition': 'Full qualified name of the target (Databricks) table'
            },
            {
                'Sheet': 'Summary',
                'Term': 'source_count',
                'Definition': 'Number of records in the source table'
            },
            {
                'Sheet': 'Summary',
                'Term': 'target_count',
                'Definition': 'Number of records in the target table'
            },
            {
                'Sheet': 'Summary',
                'Term': 'missing_in_source',
                'Definition': 'Number of records that exist in target but not in source'
            },
            {
                'Sheet': 'Summary',
                'Term': 'missing_in_target',
                'Definition': 'Number of records that exist in source but not in target'
            },
            {
                'Sheet': 'Summary',
                'Term': 'column_mismatches',
                'Definition': 'Number of matched records (identical join keys) where column values differ between source and target'
            },
            {
                'Sheet': 'Summary',
                'Term': 'threshold_mismatches',
                'Definition': 'Number of records where numeric differences exceed configured tolerance'
            },
            {
                'Sheet': 'Summary',
                'Term': 'mismatch_columns',
                'Definition': 'List of column names that have data differences'
            },
            {
                'Sheet': 'Summary',
                'Term': 'schema_match',
                'Definition': 'Boolean indicating if table structures match between systems'
            },
            {
                'Sheet': 'Summary',
                'Term': 'status',
                'Definition': 'Reconciliation result: TRUE (passed), FALSE (failed), or null (error)'
            },
            {
                'Sheet': 'Summary',
                'Term': 'exception',
                'Definition': 'Error message if reconciliation failed to execute'
            },
            {
                'Sheet': 'Summary',
                'Term': 'start_ts',
                'Definition': 'Timestamp when reconciliation started for this table'
            },
            {
                'Sheet': 'Summary',
                'Term': 'end_ts',
                'Definition': 'Timestamp when reconciliation completed for this table'
            },
            {
                'Sheet': 'Summary',
                'Term': 'duration_seconds',
                'Definition': 'Time taken to complete reconciliation in seconds'
            },
            # --- Statistics ---
            {
                'Sheet': 'Statistics',
                'Term': 'Total Tables',
                'Definition': 'Total number of tables included in this reconciliation run'
            },
            {
                'Sheet': 'Statistics',
                'Term': 'Tables with Issues',
                'Definition': 'Number of tables that have missing records or column mismatches'
            },
            {
                'Sheet': 'Statistics',
                'Term': 'Total Source Records',
                'Definition': 'Sum of all source record counts across all tables'
            },
            {
                'Sheet': 'Statistics',
                'Term': 'Total Target Records',
                'Definition': 'Sum of all target record counts across all tables'
            },
            {
                'Sheet': 'Statistics',
                'Term': 'Records with Column Mismatches',
                'Definition': 'Total count of records with column value differences across all tables'
            },
            {
                'Sheet': 'Statistics',
                'Term': 'Records Missing in Source',
                'Definition': 'Total count of records that exist in target but not source across all tables'
            },
            {
                'Sheet': 'Statistics',
                'Term': 'Records Missing in Target',
                'Definition': 'Total count of records that exist in source but not target across all tables'
            },
            # --- Column Mismatches sheet --- 
            {
                'Sheet': 'Column Mismatches',
                'Term': 'table_id',
                'Definition': 'Reference to recon_table_id for this set of samples'
            },
            {
                'Sheet': 'Column Mismatches',
                'Term': 'source_table',
                'Definition': 'Full qualified name of the source table'
            },
            {
                'Sheet': 'Column Mismatches',
                'Term': 'target_table',
                'Definition': 'Full qualified name of the target table'
            },
            {
                'Sheet': 'Column Mismatches',
                'Term': '[column]_base',
                'Definition': 'Column value from the source system (appears for each table column)'
            },
            {
                'Sheet': 'Column Mismatches',
                'Term': '[column]_compare',
                'Definition': 'Column value from the target system (appears for each table column)'
            },
            {
                'Sheet': 'Column Mismatches',
                'Term': '[column]_match',
                'Definition': 'Boolean: true if values match, false if different (appears for each table column)'
            },
            # Missing in Source sheet
            {
                'Sheet': 'Missing in Source',
                'Term': 'table_id',
                'Definition': 'Reference to recon_table_id for this set of samples'
            },
            {
                'Sheet': 'Missing in Source',
                'Term': 'source_table',
                'Definition': 'Full qualified name of the source table'
            },
            {
                'Sheet': 'Missing in Source',
                'Term': 'target_table',
                'Definition': 'Full qualified name of the target table'
            },
            {
                'Sheet': 'Missing in Source',
                'Term': '[columns]',
                'Definition': 'All target table columns showing records that exist only in target'
            },
            # Missing in Target sheet
            {
                'Sheet': 'Missing in Target',
                'Term': 'table_id',
                'Definition': 'Reference to recon_table_id for this set of samples'
            },
            {
                'Sheet': 'Missing in Target',
                'Term': 'source_table',
                'Definition': 'Full qualified name of the source table'
            },
            {
                'Sheet': 'Missing in Target',
                'Term': 'target_table',
                'Definition': 'Full qualified name of the target table'
            },
            {
                'Sheet': 'Missing in Target',
                'Term': '[columns]',
                'Definition': 'All source table columns showing records that exist only in source'
            },
            # --- Schema Differences sheet ---
            {
                'Sheet': 'Schema Differences',
                'Term': 'table_id',
                'Definition': 'Reference to recon_table_id for this table'
            },
            {
                'Sheet': 'Schema Differences',
                'Term': 'source_table',
                'Definition': 'Full qualified name of the source table'
            },
            {
                'Sheet': 'Schema Differences',
                'Term': 'target_table',
                'Definition': 'Full qualified name of the target table'
            },
            {
                'Sheet': 'Schema Differences',
                'Term': 'source_column',
                'Definition': 'Column name in the source system'
            },
            {
                'Sheet': 'Schema Differences',
                'Term': 'source_datatype',
                'Definition': 'Data type of the column in source system'
            },
            {
                'Sheet': 'Schema Differences',
                'Term': 'databricks_column',
                'Definition': 'Corresponding column name in Databricks'
            },
            {
                'Sheet': 'Schema Differences',
                'Term': 'databricks_datatype',
                'Definition': 'Data type of the column in Databricks'
            },
            {
                'Sheet': 'Schema Differences',
                'Term': 'is_valid',
                'Definition': 'Boolean: true if data types are compatible, false if incompatible'
            },
            # Common identifier
            {
                'Sheet': 'All Sheets',
                'Term': 'recon_id',
                'Definition': 'Unique identifier for the complete reconciliation run (available via metadata)'
            }
        ]
        return pd.DataFrame(definitions)

    def get_table_ids(self, recon_id: str) -> List[Tuple[int, str, str]]:
        """
        Get all table IDs for a reconciliation run.

        Args:
            recon_id: Reconciliation run identifier

        Returns:
            List of tuples (recon_table_id, source_table, target_table)
        """
        query = f"""
        SELECT DISTINCT
            main.recon_table_id,
            IF(
                ISNULL(main.source_table.`catalog`),
                CONCAT_WS('.', main.source_table.`schema`, main.source_table.table_name),
                CONCAT_WS('.', main.source_table.`catalog`, main.source_table.`schema`, main.source_table.table_name)
            ) as source_table,
            CONCAT(main.target_table.`catalog`, '.', main.target_table.`schema`, '.', main.target_table.table_name) as target_table
        FROM {self.catalog}.{self.schema}.main main
        WHERE main.recon_id = '{recon_id}'
        ORDER BY main.recon_table_id
        """

        df = self.spark.sql(query)
        return [(row.recon_table_id, row.source_table, row.target_table) for row in df.collect()]
    

    def export_to_excel(
        self,
        recon_id: str,
        output_path: str,
        include_samples: bool = True,
        max_samples_per_table: int = 400
    ) -> str:
        """
        Export reconciliation results to Excel file with multiple tabs.

        Excel structure:
        - Summary: Overall run summary from main/metrics tables
        - Statistics: Statistical summary
        - Mismatches: All column mismatches from all tables
        - Missing in Source: All records missing in source from all tables
        - Missing in Target: All records missing in target from all tables

        Args:
            recon_id: Reconciliation run identifier
            output_path: Path to save the Excel file.  Will autogenerate filename if folder is provided.
            include_samples: Whether to include sample mismatch/missing records
            max_samples_per_table: Maximum samples per table (total = tables * max_samples_per_table)

        Returns:
            Path to the created Excel file
        """

        # Check if output_path is a directory or file
        path = Path(output_path)

        if path.is_dir() or not str(path).endswith('.xlsx'):
            # It's a directory path - auto-generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"recon_{recon_id}_{timestamp}.xlsx"

            # Ensure directory exists
            if not path.exists():
                path.mkdir(parents=True, exist_ok=True)

            output_path = str(path / filename)

        # Collect all sheets to write
        sheets_to_write = []
        sheets_added = 0

        # 1. Summary tab - all tables in the reconciliation run
        summary_df = self.get_reconciliation_summary(recon_id)
        if not summary_df.empty:
            sheets_to_write.append(('Summary', summary_df))
            logger.info(f"Will add Summary tab with {len(summary_df)} tables")
            sheets_added += 1

        # 2. Statistics tab
        stats_df = self.get_statistics_summary(recon_id)
        if not stats_df.empty:
            sheets_to_write.append(('Statistics', stats_df))
            logger.info("Will add Statistics tab")
            sheets_added += 1

        # 3. Sample details organized by recon_type
        if include_samples:
            # Get all samples organized by type (one DataFrame per type)
            all_samples = self.get_all_sample_details(recon_id, max_samples_per_table)

            # Dynamically create tabs for each recon_type found in the data
            # Format sheet names nicely
            sheet_name_mapping = {
                'schema': 'Schema Differences',
                'mismatch': 'Column Mismatches',
                'missing_in_source': 'Missing in Source',
                'missing_in_target': 'Missing in Target'
            }

            for recon_type, df in all_samples.items():
                if not df.empty:
                    # Use mapped name if available, otherwise title case the type
                    sheet_name = sheet_name_mapping.get(recon_type, recon_type.replace('_', ' ').title())

                    # Ensure sheet name is within Excel's 31 character limit
                    if len(sheet_name) > 31:
                        sheet_name = sheet_name[:31]

                    sheets_to_write.append((sheet_name, df))
                    logger.info(f"Will add '{sheet_name}' tab with {len(df)} sample records from multiple tables")
                    sheets_added += 1

        # Ensure at least one sheet exists to avoid Excel error
        if sheets_added == 0:
            # Create an info sheet explaining the issue
            info_df = pd.DataFrame({
                'Message': [f'No data found for recon_id: {recon_id}'],
                'Suggestion': ['Please verify the recon_id exists and has completed successfully'],
                'Catalog': [self.catalog],
                'Schema': [self.schema]
            })
            sheets_to_write.append(('Info', info_df))
            logger.warning(f"No data sheets created. Added Info sheet for recon_id: {recon_id}")

        # Create field definitions documentation sheet
        definitions_df = self._create_field_definitions_dataframe()
        if not definitions_df.empty:
            sheets_to_write.append(('Field Definitions', definitions_df))
            logger.info("Added Field Definitions sheet")

        # Define function to write all sheets
        def write_data(writer):
            for sheet_name, df in sheets_to_write:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        # Use shared utility to write Excel file
        output_file = write_excel_file(
            excel_writer_func=write_data,
            output_path=output_path,
            auto_adjust_columns=True,
            max_column_width=50
        )

        logger.info(f"Excel report saved to: {output_file}")
        return output_file


# Convenience functions for backward compatibility and notebook usage
def get_recon_summary(spark: SparkSession, recon_id: str, catalog: str = "remorph", schema: str = "reconcile") -> pd.DataFrame:
    """Get reconciliation summary as DataFrame."""
    reporter = ReconcileReporter(spark, catalog, schema)
    return reporter.get_reconciliation_summary(recon_id)


def export_recon_report(
    spark: SparkSession,
    recon_id: str,
    output_path: str,
    catalog: str = "remorph",
    schema: str = "reconcile",
    include_samples: bool = True
) -> str:
    """Export reconciliation report to Excel."""
    reporter = ReconcileReporter(spark, catalog, schema)
    return reporter.export_to_excel(recon_id, output_path, include_samples)


def get_recent_reconciliations(spark: SparkSession, catalog: str = "remorph", schema: str = "reconcile", limit: int = 10) -> pd.DataFrame:
    """Get most recent reconciliation runs."""
    query = f"""
    SELECT DISTINCT
        main.recon_id,
        main.start_ts,
        COUNT(*) as table_count,
        MIN(metrics.run_metrics.status) as has_failures
    FROM {catalog}.{schema}.main main
    LEFT JOIN {catalog}.{schema}.metrics metrics
        ON main.recon_table_id = metrics.recon_table_id
    GROUP BY main.recon_id, main.start_ts
    ORDER BY main.start_ts DESC
    LIMIT {limit}
    """
    return spark.sql(query).toPandas()