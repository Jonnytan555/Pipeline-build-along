from functools import lru_cache
import time
import pandas as pd
import sqlalchemy as sa


class Database:
    def __init__(
        self,
        name: str,
        host: str,
        user: str = "",
        password: str = "",
        driver: str = "ODBC Driver 17 for SQL Server",
        max_cache_age_seconds=3600,
    ):
        self.name = name
        self.host = host

        if user:
            odbc = (
                f"DRIVER={{{driver}}};"
                f"SERVER={host};"
                f"DATABASE={name};"
                f"UID={user};"
                f"PWD={password};"
                "TrustServerCertificate=yes;"
            )
        else:
            odbc = (
                f"DRIVER={{{driver}}};"
                f"SERVER={host};"
                f"DATABASE={name};"
                "Trusted_Connection=yes;"
                "TrustServerCertificate=yes;"
            )

        self.engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={odbc}")
        self.max_cache_age_seconds = max_cache_age_seconds


    def tables(self):
        inspector = sa.inspect(self.engine)
        schemas = [schema for schema in inspector.get_schema_names() if not schema.startswith('db_')]
        tables = []
        for schema in schemas:
            schema_tables = inspector.get_table_names(schema=schema)
            tables.extend([f'{schema}.{table}' for table in schema_tables]) 
        return tables
        

    def select(
        self,
        table: str,
        limit: int | None = 10000,
        columns: set[str] = None,
        criteria: str = None,
        cache: bool = True,
    ) -> pd.DataFrame:
        """
        Select data from a table and cache result
        Parameters:
        table (str)         : Table to select. Including schema. Example: dbo.MyTable
        limit (int)         : Optional, Max number of rows to return. Default: 1000. If None, it will return all rows
        colums (list[str])  : Optional Columns to select
        criteria (str)      : Optional, SQL syntact logical criteria to filter the data. Example: Score > 60 AND Score < 100. Default: None
        cache (bool)        : Optional, store result in cache or not. Default: True
        """
        if not cache:
            self._select.cache_clear()

        return self._select(
            table, limit, columns, criteria, ttl_hash=self.get_ttl_hash()
        )

    def query(self, sql_query: str, cache: bool = True) -> pd.DataFrame:
        """
        Execute SQL query from db and cache result
        sql_query (str): Query to execute
        cache (bool): Optional, store result in cache or not. Default: True
        """
        if not cache:
            self._query.cache_clear()

        return self._query(sql_query, ttl_hash=self.get_ttl_hash())

    @lru_cache()
    def _query(self, sql_query: str, ttl_hash: bool = True) -> pd.DataFrame:
        del ttl_hash
        with self.engine.connect() as connection:
            df = pd.read_sql_query(sql=sa.text(sql_query), con=connection)

        return df

    @lru_cache()
    def _select(
        self,
        table: str,
        limit: int = 1000,
        columns: set[str] = None,
        criteria: str = None,
        ttl_hash: int = None,
    ) -> pd.DataFrame:

        del ttl_hash

        select_query = self._select_query_builder(table, columns, criteria, limit)

        with self.engine.connect() as connection:
            df = pd.read_sql(sql=sa.text(select_query), con=connection)

        return df

    def _select_query_builder(
        self, table: str, columns: set[str], criteria: str, limit: int
    ) -> str:
        top = f"TOP {limit}" if limit else ""
        columns = f"{','.join(columns)}" if columns else "*"
        where = f"WHERE {criteria}" if criteria else ""

        select_query = f"""
        SELECT {top} {columns} FROM {table} {where}
        """
        return select_query

    def get_ttl_hash(self):
        """Return the same value within `seconds` time period"""
        return (
            round(time.time() / self.max_cache_age_seconds)
            if self.max_cache_age_seconds
            else time.time()
        )