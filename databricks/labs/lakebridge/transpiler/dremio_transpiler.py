"""
Dremio to Databricks SQL Transpiler

Wrapper around the existing Lakebridge SQLGlot transpiler for Dremio SQL.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import nest_asyncio

from databricks.labs.lakebridge.config import TranspileConfig, TranspileResult
from databricks.labs.lakebridge.transpiler.sqlglot.sqlglot_engine import SqlglotEngine
from databricks.labs.lakebridge.helpers.dremio_utils import create_zip_archive

logger = logging.getLogger(__name__)


@dataclass
class DremioTranspileResult:
    original_sql: str
    transpiled_sql: str
    success: bool
    statement_count: int
    errors: List[str] = field(default_factory=list)


class DremioTranspiler:
    """
    Wrapper for transpiling Dremio SQL to Databricks SQL
    """

    def __init__(self, debug: bool = False):
        """
        Initialize Dremio transpiler

        Args:
            Debug: Enable debug logging
        """
        self.engine = SqlglotEngine()
        self.debug = debug
        if debug:
            logger.setLevel(logging.DEBUG)

    def transpile(self, sql: str, pretty: bool = True) -> DremioTranspileResult:
        """
        Transpile Dremio SQL to Databricks SQL

        Args:
            sql: Dremio SQL to transpile
            pretty: Format the output SQL

        Returns:
            DremioTranspileResult with transpiled SQL and any errors
        """

        # Pre-process Dremio-specific functions
        sql = self._preprocess_dremio_sql(sql)

        # Handle existing event loop (e.g., in a Databricks notebook)
        try:
            asyncio.get_running_loop()
            # Use nest_asyncio if there's a running loop
            nest_asyncio.apply()
            
        except RuntimeError:
            # No running loop, continue normally
            pass
            
        result = asyncio.run(self._async_transpile(sql))

        # Extract errors from TranspileError objects
        errors = []
        for error in result.error_list:
            if hasattr(error, 'message'):
                errors.append(error.message)
            else:
                errors.append(str(error))

        return DremioTranspileResult(
            original_sql=sql,
            transpiled_sql=result.transpiled_code,
            success=len(result.error_list) == 0,
            statement_count=result.success_count,
            errors=errors,
        )

    async def _async_transpile(self, sql: str) -> TranspileResult:
        """Internal async transpile method"""
        # Create a dummy file path for the transpiler
        file_path = Path("dremio_sql.sql")

        # Initialize the engine (no-op) for SqlglotEngine
        await self.engine.initialize(TranspileConfig())

        # Transpile from Dremio to Databricks
        result = await self.engine.transpile(
            source_dialect="dremio", 
            target_dialect="databricks",
            source_code=sql, 
            file_path=file_path
        )

        # Shutdown the engine
        await self.engine.shutdown()

        return result

    def transpile_file(self, file_path: Path, output_path: Optional[Path] = None) -> DremioTranspileResult:
        """
        Transpile a Dremio SQL file to Databricks SQL

        Args:
            file_path: Path to Dremio SQL file
            output_path: Optional path to save transpiled SQL

        Returns:
            DremioTranspileResult
        """

        try:
            sql_content = file_path.read_text(encoding='utf-8')
            result = self.transpile(sql_content)

            if output_path and result.transpiled_sql:
                output_path.write_text(result.transpiled_sql, encoding='utf-8')
                logger.info(f"Transpiled SQL saved to {output_path}")

            return result

        except Exception as e:
            logger.error(f"Failed to process file {file_path}: {e}")
            return DremioTranspileResult(
                original_sql="", 
                transpiled_sql="", 
                success=False, 
                errors=[f"File error: {e}"]
            )

    def batch_transpile(
        self, 
        input_dir: Path, 
        output_dir: Path, 
        pattern: str = "*.sql",
        zip_filename: Optional[str] = None,
    ) -> Dict[str, DremioTranspileResult]:
        """
        Transpile all Dremio SQL files in a directory

        Args:
            input_dir: Directory containing Dremio SQL files
            output_dir: Direcftory to save transpiled SQL files
            pattern: File suffix pattern to match
            zip_filename: Optional filename for creating a zip archive of transpiled SQL files.
                        If not provided, no zip archive will be created.
        """

        results = {}
        output_dir.mkdir(parents=True, exist_ok=True)
        output_files = []

        sql_files = list(input_dir.glob(pattern))
        logger.info(f"Found {len(sql_files)} SQL files to transpile")

        for sql_file in sql_files:
            relative_path = sql_file.relative_to(input_dir)
            output_name = f"{relative_path.stem}_databricks.sql"
            output_file = output_dir / relative_path.parent / output_name
            output_file.parent.mkdir(parents=True, exist_ok=True)

            logger.info(f"Transpiling {sql_file}...")
            result = self.transpile_file(sql_file, output_file)
            results[str(sql_file)] = result

            if result.success:
                logger.info(f"Successfully transpiled {sql_file}")
                output_files.append(str(output_file))
            else:
                logger.error(f"Failed to transpile {sql_file}: {result.errors}")

        # Generate error report for failed files
        self._generate_error_report(results, output_dir)

        # Create zip archive if requested
        if zip_filename and output_files:
            zip_path = output_dir / zip_filename
            logger.info(f"Creating zip archive of {len(output_files)} transpiled files...")
            zip_result = create_zip_archive(
                files=output_files,
                output_path=zip_path,
                use_relative_paths=True
            )
            if zip_result:
                logger.info(f"✓ Zip archive created: {zip_result}")
            else:
                logger.warning("Failed to create zip archive")
            
        return results

    def _generate_error_report(self, results: Dict[str, DremioTranspileResult], output_dir: Path) -> None:
        """
        Generate a simple error report for failed transpilations

        Args:
            results: Dictionary of transpilation results
            output_dir: Directory to save the error report
        """

        failed_files = []
        for file_path, result in results.items():
            if not result.success:
                failed_files.append({'file': file_path, 'errors': result.errors})

        if failed_files:
            # Create error report
            report_path = output_dir / "transpilation_errors.log"
            with open(report_path, 'w') as f:
                f.write("Dremio to Databricks SQL Transpilation Error Report \n")
                f.write("=" * 60 + "\n")
                f.write(f"Generated {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Total files processed: {len(results)}\n")
                f.write(f"Failed files: {len(failed_files)}\n")
                f.write("=" * 60 + "\n\n")

                for failed in failed_files:
                    f.write(f"FAILED: {failed['file']}\n")
                    f.write('-' * 40 + "\n")
                    for error in failed['errors']:
                        f.write(f"ERROR: {error} \n")
                    f.write("\n")

            logger.warning(f"Transpilation Errors Occurred. Error log saved to: {report_path}")

            # Generate summary
            success_count = len(results) - len(failed_files)
            logger.info(f"Transpilation complete: {success_count}/{len(results)} files successful")

        else:
            logger.info(f"All {len(results)} files transpiles successfully. No error log generated.")

    def _preprocess_dremio_sql(self, sql: str) -> str:
        """
        Pre-process Dremio-specific functions before transpilation

        Args:
            sql: Original Dremio SQL

        Returns:
            SQL with Dremio functions replaced with standard equivalents
        """
        # Store original for comparison (debugging)
        original = sql

        # Handle CONVERT_FROM(expr, 'JSON') -> PARSE_JSON(expr)
        sql = re.sub(r"CONVERT_FROM\s*\(\s*([^,]+),\s*['\"]JSON['\"]\s*\)", r"PARSE_JSON(\1)", sql, flags=re.IGNORECASE)

        # Handle CONVERT_FROM(expr,'UTF8') -> just expr (UTF8 is default)
        sql = re.sub(r"CONVERT_FROM\s*\(\s*([^,]+),\s*['\"]UTF8['\"]\s*\)", r"\1", sql, flags=re.IGNORECASE)

        # Handle CONVERT_TO(expr, 'JSON') -> TO_JSON(expr)
        sql = re.sub(r"CONVERT_TO\s*\(\s*([^,]+),\s*['\"]JSON['\"]\s*\)", r"TO_JSON(\1)", sql, flags=re.IGNORECASE)

        # Handle FLATTEN(expr) -> EXPLODE(expr)
        sql = re.sub(r"FLATTEN\s*\(", r"EXPLODE(", sql, flags=re.IGNORECASE)

        return sql

    def check_support(self) -> Dict[str, Any]:
        """
        Check if Dremio is supported by the transpiler

        Returns:
            Dictionary with support information
        """
        supported_dialects = self.engine.supported_dialects
        return {
            'dremio_supported': 'dremio' in supported_dialects,
            'databricks_supported': 'databricks' in supported_dialects,
            'all_supported_dialects': supported_dialects,
            'transpiler_name': self.engine.transpiler_name,
        }


# Convenience functions for notebook usage
def transpile_dremio_sql(sql: str) -> Dict[str, Any]:
    """
    Simple function to transpile Dremio SQL to Databricks SQL

    Args:
        sql: Dremio SQL string to transpile

    Returns:
        Dictionary with transpiled SQL and status
    """
    transpiler = DremioTranspiler()
    result = transpiler.transpile(sql)

    return {
        'sql': result.transpiled_sql,
        'success': result.success,
        'errors': result.errors,
        'statement_count': result.statement_count,
    }


def transpile_dremio_file(file_path: str, output_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Transpile a Dremio SQL file

    Args:
        file_path: Path to Dremio sql file
        output_path: Optional path to save transpiled SQL

    Returns:
        Dictionary with transpiled results
    """
    transpiler = DremioTranspiler()
    result = transpiler.transpile_file(Path(file_path), Path(output_path) if output_path else None)

    return {
        'sql': result.transpiled_sql,
        'success': result.success,
        'errors': result.errors,
        'statement_count': result.statement_count,
    }
