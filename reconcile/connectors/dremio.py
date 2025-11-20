import logging
import re
from datetime import datetime

from databricks.sdk import WorkspaceClient
from pyspark.errors import PySparkException
from pyspark.sql import DataFrame, DataFrameReader, SparkSession
from pyspark.sql.functions import col
from sqlglot import Dialect

from databricks.labs.lakebridge.reconcile.connectors.data_source import DataSource
from databricks.labs.lakebridge.reconcile.connectors.dialect_utils import DialectUtils
from databricks.labs.lakebridge.reconcile.connectors.jdbc_reader import JDBCReaderMixin
from databricks.labs.lakebridge.reconcile.connectors.models import NormalizedIdentifier
from databricks.labs.lakebridge.reconcile.connectors.secrets import SecretsMixin
from databricks.labs.lakebridge.reconcile.recon_config import JdbcReaderOptions, Schema

logger = logging.getLogger(__name__)


class DremioDataSource(DataSource, SecretsMixin, JDBCReaderMixin):
    _DRIVER = "dremio"
    _IDENTIFIER_DELIMITER = '"'
    _SCHEMA_QUERY = """SELECT column_name,
                              column_name AS col_name,
                              CASE
                                WHEN data_type = 'CHARACTER VARYING' THEN CONCAT('VARCHAR(', character_maximum_length, ')')
                                WHEN data_type = 'CHARACTER' THEN CONCAT('CHAR(', character_maximum_length, ')')
                                WHEN data_type = 'NUMERIC' AND numeric_precision IS NOT NULL AND numeric_scale IS NOT NULL
                                    THEN CONCAT('DECIMAL(', numeric_precision, ',', numeric_scale, ')')
                                WHEN data_type = 'NUMERIC' AND numeric_precision IS NOT NULL
                                    THEN CONCAT('DECIMAL(', numeric_precision, ')')
                                ELSE data_type
                              END AS data_type
                       FROM INFORMATION_SCHEMA.COLUMNS
                       WHERE LOWER(table_schema) = '{schema}'
                       AND LOWER(table_name) = '{table}'
                       ORDER BY ordinal_position"""

    def __init__(
        self,
        engine: Dialect,
        spark: SparkSession,
        ws: WorkspaceClient,
        secret_scope: str,
    ):
        self._engine = engine
        self._spark = spark
        self._ws = ws
        self._secret_scope = secret_scope

    @property
    def get_jdbc_url(self) -> str:
        
        # Get connection details from secret
        host = self._get_secret('host')
        port = self._get_secret_or_none('port') or '31010'
        database = self._get_secret_or_none('database')
        schema = self._get_secret_or_none('schema')
        user = self._get_secret('user')
        password = self._get_secret('password')

        # Support for optional SSL
        use_ssl_str = self._get_secret_or_none('use_ssl')
        use_ssl = use_ssl_str and use_ssl_str.lower() == 'true'

        # Support for optional Arrow Flight
        use_arrow_str = self._get_secret_or_none('use_arrow_flight')
        use_arrow = use_arrow_str and use_arrow_str.lower() == 'true'

        # Build connection parameters
        ssl_param = ';ssl=true' if use_ssl else ''
        arrow_param = ';useArrowFlightSQL=true' if use_arrow else ''
        schema_param = f';schema={schema}' if schema else ''

        return (
            f"jdbc:{DremioDataSource._DRIVER}:direct={host}:{port}"
            f"{schema_param}"
            f";user={user};password={password}"
            f"{ssl_param}{arrow_param}"
        )

    def read_data(
        self,
        catalog: str | None,
        schema: str,
        table: str,
        query: str,
        options: JdbcReaderOptions | None,
    ) -> DataFrame:
        # Dremio uses dot notation for schema.table
        table_query = query.replace(":tbl", f'"{schema}"."{table}"')

        try:
            if options is None:
                df = self.reader(table_query).load()
            else:
                reader_options = self._get_jdbc_reader_options(options)
                df = self.reader(table_query).options(**reader_options).load()

            logger.info(f"Fetching data using query: \n`{table_query}`")

            # Convert all column names to lower case for consistency
            df = df.select([col(c).alias(c.lower()) for c in df.columns])
            return df
        except (RuntimeError, PySparkException) as e:
            return self.log_and_throw_exception(e, "data", table_query)

    def get_schema(
        self,
        catalog: str | None,
        schema: str,
        table: str,
        normalize: bool = True,
    ) -> list[Schema]:
        schema_query = re.sub(
            r'\s+',
            ' ',
            DremioDataSource._SCHEMA_QUERY.format(schema=schema.lower(), table=table.lower()),
        )

        try:
            logger.debug(f"Fetching schema using query: \n`{schema_query}`")
            logger.info(f"Fetching Schema: Started at: {datetime.now()}")

            df = self.reader(schema_query).load()
            schema_metadata = df.select([col(c).alias(c.lower()) for c in df.columns]).collect()

            logger.info(f"Schema fetched successfully. Completed at: {datetime.now()}")
            logger.debug(f"schema_metadata: {schema_metadata}")

            return [self._map_meta_column(field, normalize) for field in schema_metadata]
        except (RuntimeError, PySparkException) as e:
            return self.log_and_throw_exception(e, "schema", schema_query)

    def reader(self, query: str) -> DataFrameReader:
        return self._get_jdbc_reader(query, self.get_jdbc_url, DremioDataSource._DRIVER, prepare_query=None)

    def normalize_identifier(self, identifier: str) -> NormalizedIdentifier:
        return DialectUtils.normalize_identifier(
            identifier,
            source_start_delimiter=DremioDataSource._IDENTIFIER_DELIMITER,
            source_end_delimiter=DremioDataSource._IDENTIFIER_DELIMITER,
        )
