# server.py
import asyncio
import logging
import argparse
import re
from typing import List, Dict, Any, Optional
from functools import partial 

import asyncmy
import anyio 
from fastmcp import FastMCP, Context

# Import configuration settings
from config import (
    DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME,
    MCP_READ_ONLY, MCP_MAX_POOL_SIZE,
    logger
)

from asyncmy.errors import Error as AsyncMyError

# --- MariaDB MCP Server Class ---
class MariaDBServer:
    """
    MCP Server exposing tools to interact with a MariaDB database.
    Manages the database connection pool.
    """
    def __init__(self, server_name="MariaDB_Server", autocommit=True):
        self.mcp = FastMCP(server_name)
        self.pool: Optional[asyncmy.Pool] = None
        self.autocommit=autocommit
        self.is_read_only = MCP_READ_ONLY
        logger.info(f"Initializing {server_name}...")
        if self.is_read_only:
            logger.warning("Server running in READ-ONLY mode. Write operations are disabled.")

    async def initialize_pool(self):
        """Initializes the asyncmy connection pool within the running event loop."""
        if not all([DB_USER, DB_PASSWORD]):
             logger.error("Cannot initialize pool due to missing database credentials.")
             raise ConnectionError("Missing database credentials for pool initialization.")

        if self.pool is not None:
            logger.info("Connection pool already initialized.")
            return

        try:
            logger.info(f"Creating connection pool for {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME} (max size: {MCP_MAX_POOL_SIZE})")
            self.pool = await asyncmy.create_pool(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                db=DB_NAME,
                minsize=1,
                maxsize=MCP_MAX_POOL_SIZE,
                autocommit=self.autocommit,
                pool_recycle=3600
            )
            logger.info("Connection pool initialized successfully.")
        except AsyncMyError as e:
            logger.error(f"Failed to initialize database connection pool: {e}", exc_info=True)
            self.pool = None
            raise
        except Exception as e:
            logger.error(f"An unexpected error occurred during pool initialization: {e}", exc_info=True)
            self.pool = None
            raise

    async def close_pool(self):
        """Closes the connection pool gracefully."""
        if self.pool:
            logger.info("Closing database connection pool...")
            try:
                # clear() is asyncmy's only COM_QUIT path; once close() sets _closing,
                # wait_closed() and release() drop the socket and the server logs an abort.
                await self.pool.clear()
                self.pool.close()
                await self.pool.wait_closed()
                logger.info("Database connection pool closed.")
            except Exception as e:
                logger.error(f"Error closing connection pool: {e}", exc_info=True)
            finally:
                self.pool = None

    async def _execute_query(self, sql: str, params: Optional[tuple] = None, database: Optional[str] = None) -> List[Dict[str, Any]]:
        """Helper function to execute SELECT queries using the pool."""
        if self.pool is None:
            logger.error("Connection pool is not initialized.")
            raise RuntimeError("Database connection pool not available.")

        allowed_prefixes = ('SELECT', 'SHOW', 'DESC', 'DESCRIBE', 'EXPLAIN', 'USE')
        
        # Strip SQL comments from query
        # Remove single-line comments (-- comment)
        sql_no_comments = re.sub(r'--.*?$', '', sql, flags=re.MULTILINE)
        # Remove multi-line comments (/* comment */)
        sql_no_comments = re.sub(r'/\*.*?\*/', '', sql_no_comments, flags=re.DOTALL)
        sql_no_comments = sql_no_comments.strip()
        
        query_upper = sql_no_comments.upper()
        is_allowed_read_query = any(query_upper.startswith(prefix) for prefix in allowed_prefixes)

        if self.is_read_only and not is_allowed_read_query:
             logger.warning(f"Blocked potentially non-read-only query in read-only mode: {sql[:100]}...")
             raise PermissionError("Operation forbidden: Server is in read-only mode.")

        logger.info(f"Executing query (DB: {database or DB_NAME}): {sql[:100]}...")
        if params:
            logger.debug(f"Parameters: {params}")

        conn = None
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor(cursor=asyncmy.cursors.DictCursor) as cursor:
                    current_db_query = "SELECT DATABASE()"
                    await cursor.execute(current_db_query)
                    current_db_result = await cursor.fetchone()
                    current_db_name = current_db_result.get('DATABASE()') if current_db_result else None
                    pool_db_name = DB_NAME
                    actual_current_db = current_db_name or pool_db_name

                    if database and database != actual_current_db:
                        logger.info(f"Switching database context from '{actual_current_db}' to '{database}'")
                        await cursor.execute(f"USE `{database}`")

                    await cursor.execute(sql, params or ())
                    results = await cursor.fetchall()
                    logger.info(f"Query executed successfully, {len(results)} rows returned.")
                    return results if results else []
        except AsyncMyError as e:
            conn_state = f"Connection: {'acquired' if conn else 'not acquired'}"
            logger.error(f"Database error executing query ({conn_state}): {e}", exc_info=True)
            # Check for specific connection-related errors if possible
            raise RuntimeError(f"Database error: {e}") from e
        except PermissionError as e:
             logger.warning(f"Permission denied: {e}")
             raise e
        except Exception as e:
            # Catch potential loop closed errors here too, although ideally fixed by structure change
            if isinstance(e, RuntimeError) and 'Event loop is closed' in str(e):
                 logger.critical("Detected closed event loop during query execution!", exc_info=True)
                 # This indicates a fundamental problem with loop management still exists
                 raise RuntimeError("Event loop closed unexpectedly during query.") from e
            conn_state = f"Connection: {'acquired' if conn else 'not acquired'}"
            logger.error(f"Unexpected error during query execution ({conn_state}): {e}", exc_info=True)
            raise RuntimeError(f"An unexpected error occurred: {e}") from e
            
    async def _database_exists(self, database_name: str) -> bool:
        """Checks if a database exists."""
        if not database_name or not database_name.isidentifier():
            logger.warning(f"_database_exists called with invalid database_name: {database_name}")
            return False 

        sql = "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = %s"
        try:
            results = await self._execute_query(sql, params=(database_name,), database='information_schema')
            return len(results) > 0
        except Exception as e:
            logger.error(f"Error checking if database '{database_name}' exists: {e}", exc_info=True)
            return False
        
    # Single query for everything get_table_schema needs: column definitions,
    # column comments and the table-level comment. A LEFT JOIN from TABLES means
    # a missing table yields no rows at all (so existence needs no extra probe),
    # while a table with no readable columns still yields its TABLE_COMMENT.
    _TABLE_METADATA_SQL = """
    SELECT
        c.COLUMN_NAME    as column_name,
        c.COLUMN_TYPE    as column_type,
        c.IS_NULLABLE    as is_nullable,
        c.COLUMN_KEY     as column_key,
        c.COLUMN_DEFAULT as column_default,
        c.EXTRA          as extra,
        c.COLUMN_COMMENT as column_comment,
        t.TABLE_TYPE     as table_type,
        t.TABLE_COMMENT  as table_comment
    FROM information_schema.TABLES t
    LEFT JOIN information_schema.COLUMNS c
           ON c.TABLE_SCHEMA = t.TABLE_SCHEMA
          AND c.TABLE_NAME   = t.TABLE_NAME
    WHERE t.TABLE_SCHEMA = %s AND t.TABLE_NAME = %s
    ORDER BY c.ORDINAL_POSITION
    """

    # MySQL/MariaDB escape sequences that may appear inside a quoted default.
    _DEFAULT_ESCAPES = {
        '0': '\0', 'b': '\b', 'n': '\n', 'r': '\r',
        't': '\t', 'Z': '\x1a', "'": "'", '"': '"', '\\': '\\',
    }

    @classmethod
    def _normalize_column_default(cls, value: Optional[str]) -> Optional[str]:
        """
        Converts an INFORMATION_SCHEMA.COLUMN_DEFAULT into the value DESCRIBE
        reports, so switching sources does not change what callers see.

        COLUMN_DEFAULT holds a SQL literal: string defaults arrive quoted
        ("'active'"), and a column with no default arrives as the unquoted
        string "NULL" rather than SQL NULL. Numeric defaults ("5") and
        expressions ("current_timestamp()") are already unquoted and pass
        through untouched. The quoting is what disambiguates a real string
        default of "NULL" ("'NULL'") from no default at all ("NULL").
        """
        if value is None or value == 'NULL':
            return None
        if len(value) >= 2 and value.startswith("'") and value.endswith("'"):
            inner = value[1:-1]
            out = []
            i = 0
            while i < len(inner):
                char = inner[i]
                if char == "'" and inner[i + 1:i + 2] == "'":
                    out.append("'")
                    i += 2
                elif char == '\\' and i + 1 < len(inner):
                    nxt = inner[i + 1]
                    out.append(cls._DEFAULT_ESCAPES.get(nxt, nxt))
                    i += 2
                else:
                    out.append(char)
                    i += 1
            return ''.join(out)
        return value

    @staticmethod
    def _table_comment_from(row: Dict[str, Any]) -> str:
        """
        Reads the table-level comment from a metadata row.

        MariaDB reports TABLE_COMMENT as the literal string 'VIEW' for views and
        offers no COMMENT clause on CREATE VIEW, so that placeholder is not
        documentation and must not be surfaced as such.
        """
        comment = row.get('table_comment') or ''
        if row.get('table_type') == 'VIEW' and comment == 'VIEW':
            return ''
        return comment

    # --- MCP Tool Definitions ---

    async def list_databases(self) -> List[str]:
        """Lists all accessible databases on the connected MariaDB server."""
        logger.info("TOOL START: list_databases called.")
        sql = "SHOW DATABASES"
        try:
            results = await self._execute_query(sql)
            db_list = [row['Database'] for row in results if 'Database' in row]
            logger.info(f"TOOL END: list_databases completed. Databases found: {len(db_list)}.")
            return db_list
        except Exception as e:
            logger.error(f"TOOL ERROR: list_databases failed: {e}", exc_info=True)
            raise

    async def list_tables(self, database_name: str) -> List[str]:
        """Lists all tables within the specified database."""
        logger.info(f"TOOL START: list_tables called. database_name={database_name}")
        if not database_name or not database_name.isidentifier():
            logger.warning(f"TOOL WARNING: list_tables called with invalid database_name: {database_name}")
            raise ValueError(f"Invalid database name provided: {database_name}")
        sql = "SHOW TABLES"
        try:
            results = await self._execute_query(sql, database=database_name)
            table_list = [list(row.values())[0] for row in results if row]
            logger.info(f"TOOL END: list_tables completed. Tables found: {len(table_list)}.")
            return table_list
        except Exception as e:
            logger.error(f"TOOL ERROR: list_tables failed for database_name={database_name}: {e}", exc_info=True)
            raise

    async def get_table_schema(self, database_name: str, table_name: str) -> Dict[str, Any]:
        """
        Retrieves the schema (column names, types, nullability, keys, default values,
        comments) for a specific table in a database, plus the table-level comment.

        Returns {'table_name': ..., 'comment': ..., 'columns': {col_name: {...}}}.
        Columns are nested under 'columns' because a table may itself have a column
        named 'comment' (e.g. information_schema.STATISTICS).
        """
        logger.info(f"TOOL START: get_table_schema called. database_name={database_name}, table_name={table_name}")
        if not database_name or not database_name.isidentifier():
            logger.warning(f"TOOL WARNING: get_table_schema called with invalid database_name: {database_name}")
            raise ValueError(f"Invalid database name provided: {database_name}")
        if not table_name or not table_name.isidentifier():
            logger.warning(f"TOOL WARNING: get_table_schema called with invalid table_name: {table_name}")
            raise ValueError(f"Invalid table name provided: {table_name}")

        try:
            metadata_rows = await self._execute_query(
                self._TABLE_METADATA_SQL, params=(database_name, table_name)
            )
            if not metadata_rows:
                logger.warning(f"TOOL WARNING: Table '{database_name}'.'{table_name}' not found or inaccessible.")
                raise FileNotFoundError(f"Table '{database_name}'.'{table_name}' not found or inaccessible.")

            schema_info = {}
            for row in metadata_rows:
                col_name = row.get('column_name')
                if col_name:
                    schema_info[col_name] = {
                        'type': row.get('column_type'),
                        'nullable': (row.get('is_nullable') or '').upper() == 'YES',
                        'key': row.get('column_key'),
                        'default': self._normalize_column_default(row.get('column_default')),
                        'extra': row.get('extra'),
                        'comment': row.get('column_comment') or ''
                    }

            if not schema_info:
                logger.warning(f"Table '{database_name}'.'{table_name}' exposed no columns. It might lack permissions.")

            result = {
                'table_name': table_name,
                'comment': self._table_comment_from(metadata_rows[0]),
                'columns': schema_info
            }
            logger.info(f"TOOL END: get_table_schema completed. Columns found: {len(schema_info)}. Keys: {list(schema_info.keys())}")
            return result
        except FileNotFoundError as e:
            logger.warning(f"TOOL WARNING: get_table_schema table not found: {e}")
            raise e
        except Exception as e:
            logger.error(f"TOOL ERROR: get_table_schema failed for database_name={database_name}, table_name={table_name}: {e}", exc_info=True)
            raise RuntimeError(f"Could not retrieve schema for table '{database_name}.{table_name}'.")
        
    async def get_table_schema_with_relations(self, database_name: str, table_name: str) -> Dict[str, Any]:
        """
        Retrieves table schema with foreign key relationship information.
        Includes all basic schema info (types, nullability, keys, defaults, comments)
        plus the table-level comment, foreign key relationships and referenced tables.
        """
        logger.info(f"TOOL START: get_table_schema_with_relations called. database_name={database_name}, table_name={table_name}")
        if not database_name or not database_name.isidentifier():
            logger.warning(f"TOOL WARNING: get_table_schema_with_relations called with invalid database_name: {database_name}")
            raise ValueError(f"Invalid database name provided: {database_name}")
        if not table_name or not table_name.isidentifier():
            logger.warning(f"TOOL WARNING: get_table_schema_with_relations called with invalid table_name: {table_name}")
            raise ValueError(f"Invalid table name provided: {table_name}")

        try:
            # 1. Get basic schema information (includes column and table comments)
            basic_schema = await self.get_table_schema(database_name, table_name)
            basic_columns = basic_schema['columns']
            
            # 2. Retrieve foreign key information
            fk_sql = """
            SELECT 
                kcu.COLUMN_NAME as column_name,
                kcu.CONSTRAINT_NAME as constraint_name,
                kcu.REFERENCED_TABLE_NAME as referenced_table,
                kcu.REFERENCED_COLUMN_NAME as referenced_column,
                rc.UPDATE_RULE as on_update,
                rc.DELETE_RULE as on_delete
            FROM information_schema.KEY_COLUMN_USAGE kcu
            INNER JOIN information_schema.REFERENTIAL_CONSTRAINTS rc
                ON kcu.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
                AND kcu.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
            WHERE kcu.TABLE_SCHEMA = %s 
              AND kcu.TABLE_NAME = %s 
              AND kcu.REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY kcu.CONSTRAINT_NAME, kcu.ORDINAL_POSITION
            """
            
            fk_results = await self._execute_query(fk_sql, params=(database_name, table_name))
            
            # 3. Add foreign key information to the basic schema
            enhanced_schema = {}
            for col_name, col_info in basic_columns.items():
                enhanced_schema[col_name] = col_info.copy()
                enhanced_schema[col_name]['foreign_key'] = None
            
            # 4. Add foreign key information to the corresponding columns
            for fk_row in fk_results:
                column_name = fk_row['column_name']
                if column_name in enhanced_schema:
                    enhanced_schema[column_name]['foreign_key'] = {
                        'constraint_name': fk_row['constraint_name'],
                        'referenced_table': fk_row['referenced_table'],
                        'referenced_column': fk_row['referenced_column'],
                        'on_update': fk_row['on_update'],
                        'on_delete': fk_row['on_delete']
                    }
            
            # 5. Return the enhanced schema with foreign key relations
            result = {
                'table_name': table_name,
                'comment': basic_schema['comment'],
                'columns': enhanced_schema
            }
            
            logger.info(f"TOOL END: get_table_schema_with_relations completed. Columns: {len(enhanced_schema)}, Foreign keys: {len(fk_results)}")
            return result
            
        except Exception as e:
            logger.error(f"TOOL ERROR: get_table_schema_with_relations failed for database_name={database_name}, table_name={table_name}: {e}", exc_info=True)
            raise RuntimeError(f"Could not retrieve schema with relations for table '{database_name}.{table_name}': {str(e)}")


    async def execute_sql(self, sql_query: str, database_name: str, parameters: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
        """
        Executes a read-only SQL query (primarily SELECT, SHOW, DESCRIBE) against a specified database
        and returns the results. Uses parameterized queries for safety.
        Example `parameters`: ["value1", 123] corresponding to %s placeholders in `sql_query`.
        """
        logger.info(f"TOOL START: execute_sql called. database_name={database_name}, sql_query={sql_query[:100]}, parameters={parameters}")
        if database_name and not database_name.isidentifier():
            logger.warning(f"TOOL WARNING: execute_sql called with invalid database_name: {database_name}")
            raise ValueError(f"Invalid database name provided: {database_name}")
        param_tuple = tuple(parameters) if parameters is not None else None
        try:
            results = await self._execute_query(sql_query, params=param_tuple, database=database_name)
            logger.info(f"TOOL END: execute_sql completed. Rows returned: {len(results)}.")
            return results
        except Exception as e:
            logger.error(f"TOOL ERROR: execute_sql failed for database_name={database_name}, sql_query={sql_query[:100]}, parameters={parameters}: {e}", exc_info=True)
            raise
            
    async def create_database(self, database_name: str) -> Dict[str, Any]:
        """
        Creates a new database if it doesn't exist.
        """
        logger.info(f"TOOL START: create_database called for database: '{database_name}'")
        if not database_name or not database_name.isidentifier():
            logger.error(f"Invalid database_name for creation: '{database_name}'. Must be a valid identifier.")
            raise ValueError(f"Invalid database_name for creation: '{database_name}'. Must be a valid identifier.")

        # Check existence first to provide a clear message, though CREATE DATABASE IF NOT EXISTS is idempotent
        if await self._database_exists(database_name):
            message = f"Database '{database_name}' already exists."
            logger.info(f"TOOL END: create_database. {message}")
            return {"status": "exists", "message": message, "database_name": database_name}

        sql = f"CREATE DATABASE IF NOT EXISTS `{database_name}`;"

        try:
            await self._execute_query(sql, database=None)

            message = f"Database '{database_name}' created successfully."
            logger.info(f"TOOL END: create_database. {message}")
            return {"status": "success", "message": message, "database_name": database_name}
        except Exception as e:
            error_message = f"Failed to create database '{database_name}'."
            logger.error(f"TOOL ERROR: create_database. {error_message} Error: {e}", exc_info=True)
            raise RuntimeError(f"{error_message} Reason: {str(e)}")

    # --- Tool Registration (Synchronous) ---
    def register_tools(self):
        """Registers the class methods as MCP tools using the instance. This is synchronous."""
        if self.pool is None:
             logger.error("Cannot register tools: Database pool is not initialized.")
             raise RuntimeError("Database pool must be initialized before registering tools.")

        @self.mcp.tool
        async def list_databases() -> List[str]:
            """Lists all accessible databases on the connected MariaDB server."""
            return await self.list_databases()
            
        @self.mcp.tool
        async def list_tables(database_name: str) -> List[str]:
            """Lists all tables within the specified database."""
            return await self.list_tables(database_name)
            
        @self.mcp.tool
        async def get_table_schema(database_name: str, table_name: str) -> Dict[str, Any]:
            """Retrieves the schema for a specific table in a database, including the
            table-level comment and a per-column comment documenting each column."""
            return await self.get_table_schema(database_name, table_name)
            
        @self.mcp.tool
        async def get_table_schema_with_relations(database_name: str, table_name: str) -> Dict[str, Any]:
            """Retrieves table schema with foreign key relationship information, including
            the table-level comment and a per-column comment documenting each column."""
            return await self.get_table_schema_with_relations(database_name, table_name)
            
        @self.mcp.tool
        async def execute_sql(sql_query: str, database_name: str, parameters: Optional[List[Any]] = None) -> List[Dict[str, Any]]:
            """Executes a read-only SQL query against a specified database."""
            return await self.execute_sql(sql_query, database_name, parameters)
            
        @self.mcp.tool
        async def create_database(database_name: str) -> Dict[str, Any]:
            """Creates a new database if it doesn't exist."""
            return await self.create_database(database_name)
            
        logger.info("Registered MCP tools explicitly.")

    # --- Async Main Server Logic ---
    async def run_async_server(self, transport="stdio", host="127.0.0.1", port=9001, path="/mcp"):
        """
        Initializes pool, registers tools, and runs the appropriate async MCP listener.
        This method should be the target for anyio.run().
        """
        try:
            # 1. Initialize pool within the anyio-managed loop
            await self.initialize_pool()

            # 2. Register tools (synchronous part, but called from async context)
            self.register_tools()

            # 3. Prepare transport arguments
            transport_kwargs = {}
            if transport == "sse":
                transport_kwargs = {"host": host, "port": port}
                logger.info(f"Starting MCP server via {transport} on {host}:{port}...")
            elif transport == "http":
                transport_kwargs = {"host": host, "port": port, "path": path}
                logger.info(f"Starting MCP server via {transport} on {host}:{port}{path}...")
            elif transport == "stdio":
                 logger.info(f"Starting MCP server via {transport}...")
            else:
                 logger.error(f"Unsupported transport type: {transport}")
                 return 

            # 4. Run the appropriate async listener from FastMCP
            await self.mcp.run_async(transport=transport, **transport_kwargs)

        except (ConnectionError, AsyncMyError, RuntimeError) as e:
            logger.critical(f"Server setup failed: {e}", exc_info=True)
            raise
        except Exception as e:
            logger.critical(f"Server execution failed with an unexpected error: {e}", exc_info=True)
            raise
        finally:
            await self.close_pool()


# --- Main Execution Block ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MariaDB MCP Server")
    parser.add_argument('--transport', type=str, default='stdio', choices=['stdio', 'sse', 'http'],
                        help='MCP transport protocol (stdio, sse, or http)')
    parser.add_argument('--host', type=str, default='127.0.0.1',
                        help='Host for SSE or HTTP transport')
    parser.add_argument('--port', type=int, default=9001,
                        help='Port for SSE or HTTP transport')
    parser.add_argument('--path', type=str, default='/mcp',
                        help='Path for HTTP transport (default: /mcp)')
    args = parser.parse_args()

    # 1. Create the server instance
    server = MariaDBServer()
    exit_code = 0

    try:
        # 2. Use anyio.run to manage the event loop and call the main async server logic
        anyio.run(
            partial(server.run_async_server, 
                    transport=args.transport, 
                    host=args.host, 
                    port=args.port, 
                    path=args.path)
        )
        logger.info("Server finished gracefully.")

    except KeyboardInterrupt:
         logger.info("Server execution interrupted by user.")
    except Exception as e:
         logger.critical(f"Server failed to start or crashed: {e}", exc_info=True)
         exit_code = 1
    finally:
        logger.info(f"Server exiting with code {exit_code}.")