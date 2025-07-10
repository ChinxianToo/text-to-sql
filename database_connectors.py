"""
Database Connection Utilities
Handles connections to MySQL, MSSQL, and PostgreSQL databases with schema introspection.
"""

import pandas as pd
import sqlalchemy as sa
from sqlalchemy import create_engine, text, inspect
from typing import Dict, List, Any, Optional, Tuple
import logging
from datetime import datetime
import traceback

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseConnector:
    """
    Universal database connector supporting MySQL, MSSQL, and PostgreSQL
    """
    
    SUPPORTED_DATABASES = {
        'mysql': {
            'driver': 'mysql+mysqlconnector',
            'default_port': 3306,
            'ssl_options': ['preferred', 'disabled', 'required']
        },
        'postgresql': {
            'driver': 'postgresql+psycopg2',
            'default_port': 5432,
            'ssl_options': ['prefer', 'disable', 'require']
        },
        'mssql': {
            'driver': 'mssql+pyodbc',
            'default_port': 1433,
            'ssl_options': ['yes', 'no']
        }
    }
    
    def __init__(self):
        self.engine = None
        self.connection_info = {}
        self.is_connected = False
    
    def create_connection_string(self, db_type: str, host: str, port: int, username: str, 
                               password: str, database: str = "", ssl: str = "preferred") -> str:
        """
        Create database connection string based on database type
        
        Args:
            db_type: Database type (mysql, postgresql, mssql)
            host: Database host
            port: Database port
            username: Username
            password: Password
            database: Database name (optional)
            ssl: SSL mode
            
        Returns:
            Connection string
        """
        if db_type not in self.SUPPORTED_DATABASES:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        db_config = self.SUPPORTED_DATABASES[db_type]
        driver = db_config['driver']
        
        # Build connection string based on database type
        if db_type == 'mysql':
            conn_str = f"{driver}://{username}:{password}@{host}:{port}"
            if database:
                conn_str += f"/{database}"
            if ssl != "preferred":
                conn_str += f"?ssl_disabled={'true' if ssl == 'disabled' else 'false'}"
                
        elif db_type == 'postgresql':
            conn_str = f"{driver}://{username}:{password}@{host}:{port}"
            if database:
                conn_str += f"/{database}"
            if ssl != "prefer":
                conn_str += f"?sslmode={ssl}"
                
        elif db_type == 'mssql':
            # MSSQL connection string is more complex
            conn_str = f"{driver}://{username}:{password}@{host}:{port}"
            if database:
                conn_str += f"/{database}"
            # Add ODBC driver parameters
            conn_str += "?driver=ODBC+Driver+17+for+SQL+Server"
            if ssl == "yes":
                conn_str += "&Encrypt=yes&TrustServerCertificate=no"
            else:
                conn_str += "&Encrypt=no"
        
        return conn_str
    
    def test_connection(self, db_type: str, host: str, port: int, username: str, 
                       password: str, database: str = "", ssl: str = "preferred") -> Tuple[bool, str]:
        """
        Test database connection
        
        Returns:
            Tuple of (success, message)
        """
        try:
            conn_str = self.create_connection_string(db_type, host, port, username, password, database, ssl)
            test_engine = create_engine(conn_str, connect_args={'connect_timeout': 10})
            
            with test_engine.connect() as conn:
                # Simple test query
                if db_type == 'mysql':
                    result = conn.execute(text("SELECT 1 as test"))
                elif db_type == 'postgresql':
                    result = conn.execute(text("SELECT 1 as test"))
                elif db_type == 'mssql':
                    result = conn.execute(text("SELECT 1 as test"))
                
                row = result.fetchone()
                if row and row[0] == 1:
                    return True, "Connection successful!"
                else:
                    return False, "Connection test failed"
                    
        except Exception as e:
            error_msg = str(e)
            if "Access denied" in error_msg:
                return False, "Access denied. Check username and password."
            elif "Unknown database" in error_msg:
                return False, f"Database '{database}' does not exist."
            elif "Connection refused" in error_msg:
                return False, f"Cannot connect to {host}:{port}. Check host and port."
            elif "timeout" in error_msg.lower():
                return False, "Connection timeout. Check network connectivity."
            else:
                return False, f"Connection failed: {error_msg}"
    
    def connect(self, db_type: str, host: str, port: int, username: str, 
                password: str, database: str = "", ssl: str = "preferred") -> bool:
        """
        Establish database connection
        
        Returns:
            True if successful, False otherwise
        """
        try:
            conn_str = self.create_connection_string(db_type, host, port, username, password, database, ssl)
            self.engine = create_engine(conn_str, pool_pre_ping=True)
            
            # Test the connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self.connection_info = {
                'db_type': db_type,
                'host': host,
                'port': port,
                'username': username,
                'database': database,
                'ssl': ssl,
                'connected_at': datetime.now()
            }
            self.is_connected = True
            logger.info(f"Connected to {db_type} database at {host}:{port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            self.is_connected = False
            return False
    
    def disconnect(self):
        """Disconnect from database"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
        self.is_connected = False
        self.connection_info = {}
        logger.info("Disconnected from database")
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Execute SQL query and return results as DataFrame
        
        Args:
            query: SQL query string
            
        Returns:
            pandas DataFrame with results
        """
        if not self.is_connected:
            raise Exception("Not connected to database. Call connect() first.")
        
        try:
            with self.engine.connect() as conn:
                result = pd.read_sql(query, conn)
            return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise
    
    def get_databases(self) -> List[str]:
        """
        Get list of available databases
        
        Returns:
            List of database names
        """
        if not self.is_connected:
            return []
        
        try:
            db_type = self.connection_info['db_type']
            
            if db_type == 'mysql':
                query = "SHOW DATABASES"
            elif db_type == 'postgresql':
                query = "SELECT datname FROM pg_database WHERE datistemplate = false"
            elif db_type == 'mssql':
                query = "SELECT name FROM sys.databases WHERE database_id > 4"  # Exclude system databases
            
            result = self.execute_query(query)
            return result.iloc[:, 0].tolist()
            
        except Exception as e:
            logger.error(f"Failed to get databases: {e}")
            return []
    
    def get_tables(self, database: str = None) -> List[Dict[str, Any]]:
        """
        Get list of tables in the database
        
        Args:
            database: Database name (optional)
            
        Returns:
            List of table information dictionaries
        """
        if not self.is_connected:
            return []
        
        try:
            inspector = inspect(self.engine)
            
            # Switch database if specified and different from current
            if database and database != self.connection_info.get('database'):
                if self.connection_info['db_type'] == 'mysql':
                    with self.engine.connect() as conn:
                        conn.execute(text(f"USE {database}"))
            
            tables = []
            for table_name in inspector.get_table_names():
                table_info = {
                    'name': table_name,
                    'type': 'table',
                    'schema': inspector.default_schema_name or 'public'
                }
                tables.append(table_info)
            
            return tables
            
        except Exception as e:
            logger.error(f"Failed to get tables: {e}")
            return []
    
    def get_table_schema(self, table_name: str, database: str = None) -> Dict[str, Any]:
        """
        Get detailed schema information for a table
        
        Args:
            table_name: Name of the table
            database: Database name (optional)
            
        Returns:
            Dictionary with table schema information
        """
        if not self.is_connected:
            return {}
        
        try:
            inspector = inspect(self.engine)
            
            # Switch database if specified
            if database and database != self.connection_info.get('database'):
                if self.connection_info['db_type'] == 'mysql':
                    with self.engine.connect() as conn:
                        conn.execute(text(f"USE {database}"))
            
            # Get columns
            columns = inspector.get_columns(table_name)
            
            # Get primary keys
            pk_constraint = inspector.get_pk_constraint(table_name)
            primary_keys = pk_constraint.get('constrained_columns', [])
            
            # Get foreign keys
            foreign_keys = inspector.get_foreign_keys(table_name)
            
            # Get indexes
            indexes = inspector.get_indexes(table_name)
            
            # Format column information
            formatted_columns = []
            for col in columns:
                col_info = {
                    'name': col['name'],
                    'type': str(col['type']),
                    'nullable': col['nullable'],
                    'default': col.get('default'),
                    'is_primary_key': col['name'] in primary_keys,
                    'is_foreign_key': any(col['name'] in fk['constrained_columns'] for fk in foreign_keys)
                }
                formatted_columns.append(col_info)
            
            return {
                'table_name': table_name,
                'columns': formatted_columns,
                'primary_keys': primary_keys,
                'foreign_keys': foreign_keys,
                'indexes': indexes
            }
            
        except Exception as e:
            logger.error(f"Failed to get table schema for {table_name}: {e}")
            return {}
    
    def get_sample_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """
        Get sample data from a table
        
        Args:
            table_name: Name of the table
            limit: Number of rows to retrieve
            
        Returns:
            DataFrame with sample data
        """
        if not self.is_connected:
            return pd.DataFrame()
        
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            if self.connection_info['db_type'] == 'mssql':
                query = f"SELECT TOP {limit} * FROM {table_name}"
            
            return self.execute_query(query)
            
        except Exception as e:
            logger.error(f"Failed to get sample data for {table_name}: {e}")
            return pd.DataFrame()
    
    def generate_schema_info(self, database: str = None) -> str:
        """
        Generate comprehensive schema information for LLM context
        
        Args:
            database: Database name (optional)
            
        Returns:
            Formatted string with database schema information
        """
        if not self.is_connected:
            return "No database connection established."
        
        try:
            tables = self.get_tables(database)
            if not tables:
                return "No tables found in the database."
            
            schema_info = f"### Database Schema Information\n"
            schema_info += f"**Database Type**: {self.connection_info['db_type'].title()}\n"
            schema_info += f"**Host**: {self.connection_info['host']}:{self.connection_info['port']}\n"
            if database:
                schema_info += f"**Database**: {database}\n"
            schema_info += f"**Tables**: {len(tables)}\n\n"
            
            for i, table in enumerate(tables[:10], 1):  # Limit to first 10 tables
                table_name = table['name']
                schema_info += f"**{i}. {table_name}**\n"
                
                # Get table schema
                table_schema = self.get_table_schema(table_name, database)
                if table_schema and table_schema.get('columns'):
                    schema_info += "   Columns:\n"
                    for col in table_schema['columns']:
                        indicators = []
                        if col['is_primary_key']:
                            indicators.append("PK")
                        if col['is_foreign_key']:
                            indicators.append("FK")
                        if not col['nullable']:
                            indicators.append("NOT NULL")
                        
                        indicator_str = f" ({', '.join(indicators)})" if indicators else ""
                        schema_info += f"   - `{col['name']}` ({col['type']}){indicator_str}\n"
                
                # Get sample data
                sample_data = self.get_sample_data(table_name, 3)
                if not sample_data.empty:
                    schema_info += f"   Sample data ({len(sample_data)} rows):\n"
                    for _, row in sample_data.iterrows():
                        row_str = " | ".join([f"{col}: {val}" for col, val in row.items()][:3])
                        schema_info += f"   {row_str}...\n"
                
                schema_info += "\n"
            
            if len(tables) > 10:
                schema_info += f"... and {len(tables) - 10} more tables\n"
            
            return schema_info
            
        except Exception as e:
            logger.error(f"Failed to generate schema info: {e}")
            return f"Error generating schema information: {str(e)}"


# Convenience functions
def create_database_connector() -> DatabaseConnector:
    """Create a new database connector instance"""
    return DatabaseConnector()


def test_database_connection(db_type: str, host: str, port: int, username: str, 
                           password: str, database: str = "", ssl: str = "preferred") -> Tuple[bool, str]:
    """
    Test database connection without creating a persistent connector
    
    Returns:
        Tuple of (success, message)
    """
    connector = DatabaseConnector()
    return connector.test_connection(db_type, host, port, username, password, database, ssl) 