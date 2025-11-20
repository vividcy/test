"""
Batch analyzer for Dremio SQL files that generates an Excel complexity report
"""

import logging
import shutil
import tempfile
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from databricks.labs.lakebridge.analyzer.dremio_analyzer import (
    CompatibilityLevel,
    ComplexityLevel,
    DremioAnalyzer,
    SQLMetrics,
)

from databricks.labs.lakebridge.helpers.dremio_utils import write_excel_file

logger = logging.getLogger(__name__)


class DremioBatchAnalyzer:
    """Batch processor for analyzing multiple Dremio SQL files"""

    def __init__(self, debug: bool = False):
        self.analyzer = DremioAnalyzer()
        self.debug = debug
        if debug:
            logging.basicConfig(level=logging.DEBUG)

    def analyze_directory(self, directory: Path, pattern: str = "*.sql") -> List[SQLMetrics]:
        """
        Analyze all SQL files in a directory

        Args:
            directory: Path to directory containing SQL files
            pattern: Glob pattern for SQL files (default: *.sql)

        Returns:
            List of SQLMetrics for each analyzed file
        """

        if not directory.exists():
            raise ValueError(f"Directory does not exist: {directory}")

        sql_files = list(directory.rglob(pattern))
        if not sql_files:
            logger.warning(f"No SQL files found in {directory} with pattern {pattern}")
            return []

        logger.info(f"found {len(sql_files)} SQL files to analyze")
        results = []

        for i, file_path in enumerate(sql_files, 1):
            logger.debug(f"Analyzing file {i}/{len(sql_files)}: {file_path}")
            try:
                metrics = self.analyzer.analyze_sql_file(file_path)
                results.append(metrics)
            except Exception as e:
                logger.error(f"Failed to analyze {file_path}: {e}")
                # Create error metrics for failed files
                error_metrics = SQLMetrics(
                    file_path=str(file_path),
                    line_count=0,
                    total_statements=0,
                    simple_statements=0,
                    conventional_statements=0,
                    loop_count=0,
                    pivot_count=0,
                    xml_count=0,
                    extracted_statements=[],
                    complexity=ComplexityLevel.LOW,
                    compatibility=CompatibilityLevel.LOW,
                    parse_errors=[str(e)],
                    dependencies=set(),
                )
                results.append(error_metrics)

        return results

    def generate_excel_report(self, metrics_list: List[SQLMetrics], output_path: Path):
        """
        Generate Excel report with summary and file detail tabs

        Args:
            metrics_list: List of FileMetrics from analysis
            output_path: Path for output Excel file
        """
        if not metrics_list:
            logger.warning("No Metrics to report")
            return

        # Create summary dataframe
        summary_df = self._create_summary_dataframe(metrics_list)

        # Create dataframe for individual file details
        detail_df = self._create_detail_dataframe(metrics_list)

        # Create dependencies data
        dependencies_df = self._create_dependencies_dataframe(metrics_list)

        # Create SQL constructs data
        sql_constructs_df = self._create_sql_constructs_dataframe(metrics_list)

        # Create field definitions documentation
        definitions_df = self._create_field_definitions_dataframe()

        # Define function to write Excel data
        def write_data(writer):
            summary_df.to_excel(writer, sheet_name='Summary', index=False)
            detail_df.to_excel(writer, sheet_name='Details', index=False)
            dependencies_df.to_excel(writer, sheet_name='Dependencies', index=False)
            sql_constructs_df.to_excel(writer, sheet_name='SQL Constructs', index=False)
            definitions_df.to_excel(writer, sheet_name='Field Definitions', index=False)

        # Use shared utility to write Excel file
        write_excel_file(
            excel_writer_func=write_data,
            output_path=output_path,
            auto_adjust_columns=True,
            max_column_width=50
        )

        logger.info(f"Report generated: {output_path}")

    def _create_summary_dataframe(self, metrics_list: List[SQLMetrics]) -> pd.DataFrame:
        """Create summary metrics dataframe"""

        total_files = len(metrics_list)
        successfully_parsed = sum(1 for m in metrics_list if not m.parse_errors)
        failed_parse = total_files - successfully_parsed

        # Complexity distribution
        complexity_dist = self._count_distribution(metrics_list, 'complexity', ComplexityLevel)

        # Compatibility distribution
        compatibility_dist = self._count_distribution(metrics_list, 'compatibility', CompatibilityLevel)

        # Aggregate statistics
        total_lines = sum(m.line_count for m in metrics_list)
        total_statements = sum(m.total_statements for m in metrics_list)
        total_simple = sum(m.simple_statements for m in metrics_list)
        total_conventional = sum(m.conventional_statements for m in metrics_list)

        # Dremio specific functions used
        all_functions = set()
        for m in metrics_list:
            all_functions.update(m.dremio_functions_found)

        # Collect all unique dependencies
        all_dependencies = set()
        for m in metrics_list:
            all_dependencies.update(m.dependencies)

        summary_data = {
            'Metric': [
                'Total Files Analyzed',
                'Successfully Parsed',
                'Failed to Parse',
                'Total Lines of Code',
                'Total SQL Statements',
                'Total Simple Statements',
                'Total Conventional Statements (Total - Simple)',
                'Files with Loops',
                'Files with Pivots',
                'Files with XML',
                'Files with Dependencies',
                'Unique Dependencies',
                '',
                '--- Complexity Distribution ---',
                'LOW',
                'MEDIUM',
                'COMPLEX',
                'VERY COMPLEX',
                '',
                '--- Compatibility Distribution ---',
                'HIGH',
                'MEDIUM',
                'LOW',
                '',
                'Dremio Specific Functions Found',
                'Analysis Date',
            ],
            'Value': [
                total_files,
                successfully_parsed,
                failed_parse,
                total_lines,
                total_statements,
                total_simple,
                total_conventional,
                sum(1 for m in metrics_list if m.loop_count > 0),
                sum(1 for m in metrics_list if m.pivot_count > 0),
                sum(1 for m in metrics_list if m.xml_count > 0),
                sum(1 for m in metrics_list if m.dependencies),
                len(all_dependencies),
                '',
                '',
                complexity_dist.get(ComplexityLevel.LOW, 0),
                complexity_dist.get(ComplexityLevel.MEDIUM, 0),
                complexity_dist.get(ComplexityLevel.COMPLEX, 0),
                complexity_dist.get(ComplexityLevel.VERY_COMPLEX, 0),
                '',
                '',
                compatibility_dist.get(CompatibilityLevel.HIGH, 0),
                compatibility_dist.get(CompatibilityLevel.MEDIUM, 0),
                compatibility_dist.get(CompatibilityLevel.LOW, 0),
                '',
                ', '.join(sorted(all_functions)) if all_functions else 'None',
                datetime.now().strftime('%Y-%m-%d %H:%M%:%S'),
            ],
        }

        return pd.DataFrame(summary_data)

    def _create_detail_dataframe(self, metrics_list: List[SQLMetrics]) -> pd.DataFrame:
        """Create dataframe of individual SQL file metrics"""

        details = []
        for m in metrics_list:
            view_name = Path(m.file_path).stem
            detail = {
                'View Name': view_name,
                'Line Count': m.line_count,
                'Successfully Parsed': (len(m.parse_errors) == 0),
                'Error Count': len(m.parse_errors),
                'Total Statements': m.total_statements,
                'Simple Statements': m.simple_statements,
                'Conventional Statements': m.conventional_statements,
                'Loop Count': m.loop_count,
                'Pivot Count': m.pivot_count,
                'XML Count': m.xml_count,
                'Complexity': m.complexity.value,
                'Compatibility': m.compatibility.value,
                'Dependency Count': len(m.dependencies),
                'Dependencies': ', '.join(sorted(m.dependencies)) if m.dependencies else '',
                'Statement Constructs': ', '.join(sorted(set(m.extracted_statements))),
                'Dremio Functions': ', '.join(sorted(set(m.dremio_functions_found))),
                'Compatibility Issues': '; '.join(sorted(set(m.compatibility_issues))),
                'Parsing Errors': '; '.join(sorted(set(m.parse_errors))) if m.parse_errors else '',
            }
            details.append(detail)

        df = pd.DataFrame(details)

        # Sort by complexity
        complexity_order = {'VERY_COMPLEX': 4, 'COMPLEX': 3, 'MEDIUM': 2, 'LOW': 1}
        df['_complexity_order'] = df['Complexity'].map(complexity_order)
        df = df.sort_values(by=['_complexity_order', 'View Name'], ascending=[False, True])
        df = df.drop('_complexity_order', axis=1)

        return df

    def _count_distribution(self, metrics_list: List[SQLMetrics], field: str, enum_class) -> Dict[Any, int]:
        """Count distribution of an enum field"""
        distribution = {}
        for level in enum_class:
            count = sum(1 for m in metrics_list if getattr(m, field) == level)
            distribution[level] = count
        return distribution

    def _create_dependencies_dataframe(self, metrics_list: List[SQLMetrics]) -> pd.DataFrame:
        """Create dataframe showing table/view dependencies across files"""
        # Build a map of dependency -> list of files that reference it
        dependency_map = {}

        for m in metrics_list:
            for dep in m.dependencies:
                if dep not in dependency_map:
                    dependency_map[dep] = []
                file_name = Path(m.file_path).name
                dependency_map[dep].append(file_name)

        # Create dependency dataframe
        dependency_data = []
        for dep, files in dependency_map.items():
            dependency_data.append(
                {"Dependency": dep, "Reference Count": len(files), "Files": ', '.join(sorted(files))}
            )
        df = pd.DataFrame(dependency_data)
        if not df.empty:
            df = df.sort_values(by=["Reference Count"], ascending=[False])

        return df
    
    def _create_sql_constructs_dataframe(self, metrics_list: List[SQLMetrics]) -> pd.DataFrame:
        """Create dataframe showing SQL construct usage across files"""
        # Build a map of SQL construct -> count and list of files
        construct_map = {}

        for m in metrics_list:
            # Use just the file name for brevity
            file_name = Path(m.file_path).stem

            # Count occurrences of each construct in this file
            construct_counts = {}
            for stmt_type in m.extracted_statements:
                construct_counts[stmt_type] = construct_counts.get(stmt_type, 0) + 1

            # Add to global construct map
            for construct, count in construct_counts.items():
                if construct not in construct_map:
                    construct_map[construct] = {'total_count': 0, 'files': set()}

                construct_map[construct]['total_count'] += count
                construct_map[construct]['files'].add(file_name)

        # Create dataframe data
        construct_data = []
        for construct, info in construct_map.items():
            construct_data.append({
                'SQL Construct': construct,
                'Total Occurrences': info['total_count'],
                'Files Using Construct': len(info['files'])
            })

        # Create dataframe and sort by total occurrences descending
        df = pd.DataFrame(construct_data)
        if not df.empty:
            df = df.sort_values('Total Occurrences', ascending=False)

        return df
    
    def _create_field_definitions_dataframe(self) -> pd.DataFrame:
        """Create documentation for report fields"""

        definitions = [
            # --- Summary ---
            {
                'Sheet': 'Summary',
                'Term': 'Total Files Analyzed',
                'Definition': 'Number of SQL files processed in this analysis'
            },
            {
                'Sheet': 'Summary',
                'Term': 'Successfully Parsed',
                'Definition': 'Number of files that were successfully parsed and analyzed'
            },
            {
                'Sheet': 'Summary',
                'Term': 'Failed to Parse',
                'Definition': 'Number of files that could not be parsed due to errors'
            },
            {
                'Sheet': 'Summary',
                'Term': 'Total Lines of Code',
                'Definition': 'Combined line count across all analyzed files'
            },
            {
                'Sheet': 'Summary',
                'Term': 'Total SQL Statements',
                'Definition': 'Total number of complete SQL commands across all files'
            },
            {
                'Sheet': 'Summary',
                'Term': 'Total Simple Statements',
                'Definition': 'Basic SQL statements without complex features like CTEs, subqueries, window functions, or set operations'
            },
            {
                'Sheet': 'Summary',
                'Term': 'Total Conventional Statements',
                'Definition': 'SQL statements with complex features (calculated as Total Statements - Simple Statements)'
            },
            {
                'Sheet': 'Summary',
                'Term': 'Files with Loops',
                'Definition': 'Number of files containing procedural loop constructs (Note: Not typically found in Dremio views)'
            },
            {
                'Sheet': 'Summary',
                'Term': 'Files with Pivots',
                'Definition': 'Number of files containing PIVOT operations'
            },
            {
                'Sheet': 'Summary',
                'Term': 'Files with XML',
                'Definition': 'Number of files containing XML-related operations'
            },
            {
                'Sheet': 'Summary',
                'Term': 'Files with Dependencies',
                'Definition': 'Number of files that reference other database objects'
            },
            {
                'Sheet': 'Summary',
                'Term': 'Total Unique Dependencies',
                'Definition': 'Count of distinct database objects referenced across all files'
            },
            {
                'Sheet': 'Summary',
                'Term': 'LOW Complexity',
                'Definition': 'Files with <1000 simple statements, ≤10 conventional statements, no loops/pivots/XML'
            },
            {
                'Sheet': 'Summary',
                'Term': 'MEDIUM Complexity',
                'Definition': 'Files with 1000-2000 simple OR 11-30 conventional OR 1+ loops OR 1-3 pivots/XML operations'
            },
            {
                'Sheet': 'Summary',
                'Term': 'COMPLEX',
                'Definition': 'Files with 2000-5000 simple OR 31-50 conventional OR 6-8 loops OR 4-5 pivots/XML operations'
            },
            {
                'Sheet': 'Summary',
                'Term': 'VERY_COMPLEX',
                'Definition': 'Files with >5000 simple OR >50 conventional OR >8 loops OR >5 pivots/XML operations'
            },
            {
                'Sheet': 'Summary',
                'Term': 'HIGH Compatibility',
                'Definition': 'Files with minimal Dremio-specific functions; mostly standard SQL requiring little to no modification'
            },
            {
                'Sheet': 'Summary',
                'Term': 'MEDIUM Compatibility',
                'Definition': 'Files with 1 high-impact OR 2+ medium-impact Dremio functions requiring moderate SQL modifications'
            },
            {
                'Sheet': 'Summary',
                'Term': 'LOW Compatibility',
                'Definition': 'Files with 2+ high-impact OR (1 high + 2 medium) impact functions requiring significant SQL rewriting'
            },
            {
                'Sheet': 'Summary',
                'Term': 'Unique Dremio Functions Found',
                'Definition': 'List of all distinct Dremio-specific functions found across all files'
            },
            {
                'Sheet': 'Summary',
                'Term': 'HIGH Impact Dremio Functions',
                'Definition': 'Complex translations needed: CONVERT_FROM, CONVERT_TO, FLATTEN'
            },
            {
                'Sheet': 'Summary',
                'Term': 'MEDIUM Dremio Impact Functions',
                'Definition': 'Moderate translation complexity: LISTAGG, CONVERT_TIMEZONE, REGEXP_SUBSTR'
            },
            {
                'Sheet': 'Summary',
                'Term': 'LOW Impact Dremio Functions',
                'Definition': 'Simple replacements: ARRAY_AGG, DATE_PART, APPROX_COUNT_DISTINCT, REGEXP_LIKE'
            },
            # --- Details ---
            {
                'Sheet': 'Details',
                'Term': 'View Name',
                'Definition': 'Name of the SQL file/view being analyzed'
            },
            {
                'Sheet': 'Details',
                'Term': 'Line Count',
                'Definition': 'Number of lines in this SQL file'
            },
            {
                'Sheet': 'Details',
                'Term': 'Total Statements',
                'Definition': 'Number of complete SQL commands in this file'
            },
            {
                'Sheet': 'Details',
                'Term': 'Simple Statements',
                'Definition': 'Count of basic SQL statements without complex features'
            },
            {
                'Sheet': 'Details',
                'Term': 'Conventional Statements',
                'Definition': 'Count of SQL statements with complex features (Total - Simple)'
            },
            {
                'Sheet': 'Details',
                'Term': 'Loop Count',
                'Definition': 'Number of procedural loop constructs in this file'
            },
            {
                'Sheet': 'Details',
                'Term': 'Pivot Count',
                'Definition': 'Number of PIVOT operations in this file'
            },
            {
                'Sheet': 'Details',
                'Term': 'XML Count',
                'Definition': 'Number of XML-related operations in this file'
            },
            {
                'Sheet': 'Details',
                'Term': 'Complexity',
                'Definition': 'Complexity level of this file (LOW/MEDIUM/COMPLEX/VERY_COMPLEX)'
            },
            {
                'Sheet': 'Details',
                'Term': 'Compatibility',
                'Definition': 'Databricks compatibility level (HIGH/MEDIUM/LOW)'
            },
            {
                'Sheet': 'Details',
                'Term': 'Dependency Count',
                'Definition': 'Number of database objects referenced by this file'
            },
            {
                'Sheet': 'Details',
                'Term': 'Dependencies',
                'Definition': 'List of database objects (tables/views) referenced'
            },
            {
                'Sheet': 'Details',
                'Term': 'Statement Types',
                'Definition': 'Types of SQL operations found (first 5 shown)'
            },
            {
                'Sheet': 'Details',
                'Term': 'Dremio Functions',
                'Definition': 'Dremio-specific functions requiring translation'
            },
            {
                'Sheet': 'Details',
                'Term': 'Compatibility Issues',
                'Definition': 'Specific functions/features needing modification (first 3 shown)'
            },
            {
                'Sheet': 'Details',
                'Term': 'Parse Errors',
                'Definition': 'Errors encountered while parsing this file'
            },
            # --- Dependencies ---
            {
                'Sheet': 'Dependencies',
                'Term': 'Dependency',
                'Definition': 'Name of a database object (table/view) referenced by SQL files'
            },
            {
                'Sheet': 'Dependencies',
                'Term': 'Reference Count',
                'Definition': 'Number of files that reference this dependency'
            },
            {
                'Sheet': 'Dependencies',
                'Term': 'Files',
                'Definition': 'List of files that reference this dependency'
            },
            # --- SQL Constructs ---
            {
                'Sheet': 'SQL Constructs',
                'Term': 'SQL Construct',
                'Definition': 'Type of SQL operation identified by the parser (e.g., Select, Join, Union, CTE, Subquery)'
            },
            {
                'Sheet': 'SQL Constructs',
                'Term': 'Total Occurrences',
                'Definition': 'Total count of this construct type across all analyzed files'
            },
            {
                'Sheet': 'SQL Constructs',
                'Term': 'Files Using Construct',
                'Definition': 'Number of files that contain this SQL construct'
            },
        ]

        return pd.DataFrame(definitions)

    def run(self, source_dir: str, output_file: str = None):
        """
        Main entry point to run the Dremio batch analysis

        Args:
            source_dir: Directory containing SQL files
            output_file: Output Excel file path (optional)
        """

        source_path = Path(source_dir)

        if not source_path.exists():
            raise ValueError(f"Source Directory does not exist: {source_path}")

        # Default output file name
        if output_file is None:
            output_file = source_path / f"dremio_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        else:
            output_file = Path(output_file)
            if not output_file.suffix:
                output_file = output_file.with_suffix(".xlsx")

        logger.info(f"Starting Dremio SQL analysis for directory: {source_path}")

        # Analayze all SQL files in directory
        metrics = self.analyze_directory(source_path)

        if metrics:
            # Generate analysis report
            self.generate_excel_report(metrics, output_file)
            logger.info(f"Analysis Complete - Report saved to {output_file}")
        else:
            logger.warning("No SQL files were analyzed")

        return metrics
