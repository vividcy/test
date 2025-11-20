"""
Dremio SQL Analyzer
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set

import sqlglot
from sqlglot import expressions as exp
from sqlglot import parse
from sqlglot.dialects import Dremio

logger = logging.getLogger(__name__)


class ComplexityLevel(Enum):
    """SQL complexity levels following Lakebridge native analyzer logic"""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    COMPLEX = "COMPLEX"
    VERY_COMPLEX = "VERY_COMPLEX"


class CompatibilityLevel(Enum):
    """Migration compatibility levels"""

    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


@dataclass
class SQLMetrics:
    """Metrics for single SQL file"""

    file_path: str
    line_count: int
    total_statements: int
    simple_statements: int
    conventional_statements: int
    loop_count: int
    pivot_count: int
    xml_count: int
    extracted_statements: List[str]
    complexity: ComplexityLevel
    compatibility: CompatibilityLevel
    dremio_functions_found: Set[str] = field(default_factory=set)
    compatibility_issues: List[str] = field(default_factory=list)
    parse_errors: List[str] = field(default_factory=list)
    dependencies: Set[str] = field(default_factory=set)


class DremioAnalyzer:
    """Analyzer for Dremio SQL files"""

    # Simple statment types (basic SQL operations)
    SIMPLE_STATEMENT_TYPES = {
        exp.Select,
        exp.Insert,
        exp.Update,
        exp.Delete,
        exp.Create,
        exp.Drop,
        exp.Alter,
        exp.Merge,
    }

    # Complex features that make a statment non-simple
    COMPLEX_FEATURES = {
        exp.CTE,
        exp.Subquery,
        exp.Window,
        exp.Lateral,
        exp.Unnest,
        exp.Union,
        exp.Intersect,
        exp.Except,
    }

    # Proceedural loop constructs
    # LOOP_CONSTRUCTS = {exp.While, exp.Loop, exp.For, exp.ForIn, exp.Repeat}
    LOOP_CONSTRUCTS = set()

    # Pivot Constructs
    PIVOT_CONSTRUCTS = {exp.Pivot}

    # Dremio-specific functions and translation complexity
    DREMIO_FUNCTIONS = {
        'CONVERT_FROM': 'HIGH',
        'CONVERT_TO': 'HIGH',
        'FLATTEN': 'HIGH',
        'ARRAY_AGG': 'LOW',
        'LISTAGG': 'MEDIUM',
        'CONVERT_TIMEZONE': 'MEDIUM',
        'DATE_PART': 'LOW',
        'APPROX_COUNT_DISTINCT': 'LOW',
        'REGEXP_LIKE': 'LOW',
        'REGEXP_SUBSTR': 'MEDIUM',
        'ARRAY_CONTAINS': 'LOW',
        'ARRAY_POSITION': 'LOW',
    }

    def analyze_sql_file(self, file_path: Path) -> SQLMetrics:
        """
        Analyze single SQL file and return metrics
        """
        try:
            sql_content = file_path.read_text(encoding='utf-8')
        except Exception as e:
            logger.error(f"Failed to read {file_path}: {e}")
            return self._create_error_metrics(str(file_path), str(e))

        # Count lines
        line_count = len(sql_content.splitlines())

        # Parse SQL
        try:
            parsed = parse(sql_content, read=Dremio, error_level="IGNORE")
            if not parsed:
                parsed = []
        except Exception as e:
            logger.warning(f"Parse error in {file_path}: {e}")
            return self._create_error_metrics(str(file_path), f"Parse error: {str(e)}", line_count)

        # Extract metrics
        total_statements = len(parsed)
        simple_count = self._count_simple_statements(parsed)
        conventional_count = total_statements - simple_count

        # Count specific constructs for complexity scoring
        loop_count = self._count_loops(parsed)
        pivot_count = self._count_pivots(parsed)
        xml_count = self._count_xml_statements(parsed)

        # Extract statement types
        extracted_statements = self._extract_statements(parsed)

        # Extract dependencies
        dependencies = self._extract_dependencies(parsed)

        # Extract Dremio-specific functions
        dremio_functions, issues = self._find_dremio_functions(parsed)

        # Calculate complexity using Lakebridge logic
        complexity = self._calculate_complexity(simple_count, conventional_count, loop_count, pivot_count, xml_count)

        compatibility = self._calculate_compatibility(dremio_functions)

        return SQLMetrics(
            file_path=str(file_path),
            line_count=line_count,
            total_statements=total_statements,
            simple_statements=simple_count,
            conventional_statements=conventional_count,
            loop_count=loop_count,
            pivot_count=pivot_count,
            xml_count=xml_count,
            extracted_statements=extracted_statements,
            complexity=complexity,
            compatibility=compatibility,
            dremio_functions_found=dremio_functions,
            compatibility_issues=issues,
            parse_errors=[],
            dependencies=dependencies,
        )

    def _count_simple_statements(self, statements: List[exp.Expression]) -> int:
        count = 0
        for stmt in statements:
            # Check if root statement is a simple type
            if type(stmt) in self.SIMPLE_STATEMENT_TYPES:
                # Only count as simple if it doesn't have complex features
                if not self._has_complex_features(stmt):
                    count += 1
        return count

    def _has_complex_features(self, stmt: exp.Expression) -> bool:
        """Check if statement has complex features"""
        for node in stmt.walk():
            # Check if node is any of the complex feature types
            if type(node) in self.COMPLEX_FEATURES:
                return True
        return False

    def _count_loops(self, statements: List[exp.Expression]) -> int:
        """Count loop constructs in SQL"""
        count = 0
        for stmt in statements:
            for node in stmt.walk():
                if type(node) in self.LOOP_CONSTRUCTS:
                    count += 1
        return count

    def _count_pivots(self, statements: List[exp.Expression]) -> int:
        """Count PIVOT statements"""
        count = 0
        for stmt in statements:
            for node in stmt.walk():
                if type(node) in self.PIVOT_CONSTRUCTS:
                    count += 1
        return count

    def _count_xml_statements(self, statements: List[exp.Expression]) -> int:
        """Count XML related SQL statements"""
        count = 0
        xml_patterns = ['FOR XML', 'XMLAGG', 'XMLELEMENT', 'XMLFOREST', 'XMLPARSE', 'XMLSERIALIZE', 'XMLTABLE']
        for stmt in statements:
            stmt_str = str(stmt).upper()
            for pattern in xml_patterns:
                if pattern in stmt_str:
                    count += 1
        return count

    def _extract_statements(self, statements: List[exp.Expression]) -> List[str]:
        """Extract individual SQL statements"""
        extracted_statements = []
        for stmt in statements:
            for node in stmt.walk():
                node_type = type(node).__name__
                extracted_statements.append(node_type)
        return extracted_statements

    def _find_dremio_functions(self, statements: List[exp.Expression]) -> tuple[Set[str], List[str]]:
        """Find Dremio-specific functions in the SQL"""
        found = set()
        issues = []

        for stmt in statements:
            for node in stmt.walk():
                if isinstance(node, exp.Func):
                    # Get function name
                    func_name = None
                    if hasattr(node, 'name'):
                        func_name = str(node.name).upper()
                    elif hasattr(node, 'this'):
                        func_name = str(node.this).upper()

                    if func_name and func_name in self.DREMIO_FUNCTIONS:
                        found.add(func_name)
                        severity = self.DREMIO_FUNCTIONS[func_name]
                        issues.append(f"{func_name} ({severity} impact)")
        return found, issues

    def _extract_dependencies(self, statements: List[exp.Expression]) -> Set[str]:
        """Extract table/view dependencies from SQL statements"""
        dependencies = set()

        for stmt in statements:
            for node in stmt.walk():
                if isinstance(node, exp.Table):
                    table_parts = []

                    if hasattr(node, 'catalog') and node.catalog:
                        table_parts.append(str(node.catalog))
                    if hasattr(node, 'db') and node.db:
                        table_parts.append(str(node.db))
                    if hasattr(node, 'this') and node.this:
                        table_parts.append(str(node.this))

                    if table_parts:
                        full_name = '.'.join(table_parts)
                        dependencies.add(full_name)

        return dependencies

    def _calculate_complexity(
        self, simple: int, conventional: int, loops: int, pivots: int, xml: int
    ) -> ComplexityLevel:
        """
        Calculate complexity level using Lakebridge native analyzer logic
        """

        # Check VERY_COMPLEX Conditions
        if loops > 8 or conventional > 50 or simple > 5000 or pivots > 5 or xml > 5:
            return ComplexityLevel.VERY_COMPLEX

        # Check COMPLEX conditions
        if loops > 5 or conventional > 30 or simple > 2000 or pivots > 3 or xml > 3:
            return ComplexityLevel.COMPLEX

        # Check MEDIUM conditions
        if loops >= 1 or conventional > 10 or simple > 1000 or pivots >= 1 or xml >= 1:
            return ComplexityLevel.MEDIUM

        # Default complexity LOW
        return ComplexityLevel.LOW

    def _calculate_compatibility(self, dremio_functions: Set[str]) -> CompatibilityLevel:
        """Calculate compatibility based on Dremio-specific functions found"""
        if not dremio_functions:
            return CompatibilityLevel.HIGH

        high_impact = sum(1 for f in dremio_functions if self.DREMIO_FUNCTIONS.get(f) == 'HIGH')
        medium_impact = sum(1 for f in dremio_functions if self.DREMIO_FUNCTIONS.get(f) == 'MEDIUM')

        if high_impact >= 2 or (high_impact >= 1 and medium_impact >= 2):
            return CompatibilityLevel.LOW
        elif high_impact >= 1 and medium_impact >= 2:
            return CompatibilityLevel.MEDIUM
        else:
            return CompatibilityLevel.HIGH

    def _create_error_metrics(self, file_path: str, error: str, line_count: int = 0) -> SQLMetrics:
        """Create metrics for a file that couldn't be parsed"""
        return SQLMetrics(
            file_path=file_path,
            line_count=line_count,
            total_statements=0,
            simple_statements=0,
            conventional_statements=0,
            loop_count=0,
            pivot_count=0,
            xml_count=0,
            extracted_statements=[],
            complexity=ComplexityLevel.LOW,
            compatibility=CompatibilityLevel.LOW,
            parse_errors=[error],
            dependencies=set(),
        )
