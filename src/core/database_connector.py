import oracledb
import pyodbc
import pandas as pd
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass
from loguru import logger
import streamlit as st


@dataclass
class ColumnInfo:
    column_name: str
    data_type: str
    nullable: bool
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    is_primary_key: bool = False
    is_unique: bool = False
    default_value: Optional[str] = None


@dataclass
class TableInfo:
    table_name: str
    schema_name: str
    table_type: str  # 'TABLE' or 'VIEW'
    columns: List[ColumnInfo]
    primary_keys: List[str]
    unique_constraints: List[List[str]]
    row_count: Optional[int] = None


class DatabaseConnector:
    def __init__(self):
        self.connection = None
        self.db_type = None
        
    def connect_oracle(self, username: str, password: str, dsn: str) -> bool:
        try:
            self.connection = oracledb.connect(
                user=username,
                password=password,
                dsn=dsn
            )
            self.db_type = 'oracle'
            logger.info(f"Successfully connected to Oracle database: {dsn}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Oracle database: {e}")
            st.error(f"Oracle connection failed: {e}")
            return False
    
    def connect_sqlserver(self, server: str, database: str, trusted: bool = True, 
                         username: str = None, password: str = None) -> bool:
        try:
            if trusted:
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"Trusted_Connection=yes;"
                    f"TrustServerCertificate=yes;"
                )
            else:
                connection_string = (
                    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                    f"SERVER={server};"
                    f"DATABASE={database};"
                    f"UID={username};"
                    f"PWD={password};"
                    f"TrustServerCertificate=yes;"
                )
            
            self.connection = pyodbc.connect(connection_string)
            self.db_type = 'sqlserver'
            logger.info(f"Successfully connected to SQL Server database: {server}/{database}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to SQL Server database: {e}")
            st.error(f"SQL Server connection failed: {e}")
            return False
    
    def validate_connection(self) -> bool:
        if not self.connection:
            return False
        
        try:
            cursor = self.connection.cursor()
            if self.db_type == 'oracle':
                cursor.execute("SELECT 1 FROM DUAL")
            else:
                cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            return True
        except Exception as e:
            logger.error(f"Connection validation failed: {e}")
            return False
    
    def get_schemas(self) -> List[str]:
        if not self.validate_connection():
            return []
        
        try:
            cursor = self.connection.cursor()
            
            if self.db_type == 'oracle':
                cursor.execute("""
                    SELECT DISTINCT owner
                    FROM all_tables
                    WHERE owner NOT IN ('SYS', 'SYSTEM', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'WMSYS', 'XDB', 'APEX_030200')
                    ORDER BY owner
                """)
            else:
                cursor.execute("""
                    SELECT SCHEMA_NAME
                    FROM INFORMATION_SCHEMA.SCHEMATA
                    WHERE SCHEMA_NAME NOT IN ('sys', 'information_schema', 'db_owner', 'db_accessadmin', 'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 'db_datawriter', 'db_denydatareader', 'db_denydatawriter')
                    ORDER BY SCHEMA_NAME
                """)
            
            schemas = [row[0] for row in cursor.fetchall()]
            cursor.close()
            return schemas
        except Exception as e:
            logger.error(f"Failed to get schemas: {e}")
            return []
    
    def get_tables_and_views(self, schema: str = None) -> List[Dict[str, Any]]:
        if not self.validate_connection():
            return []
        
        try:
            cursor = self.connection.cursor()
            
            if self.db_type == 'oracle':
                if schema:
                    cursor.execute("""
                        SELECT table_name, 'TABLE' as table_type, owner as schema_name
                        FROM all_tables
                        WHERE owner = :schema
                        UNION ALL
                        SELECT view_name, 'VIEW' as table_type, owner as schema_name
                        FROM all_views
                        WHERE owner = :schema
                        ORDER BY table_name
                    """, {'schema': schema})
                else:
                    cursor.execute("""
                        SELECT table_name, 'TABLE' as table_type, owner as schema_name
                        FROM all_tables
                        WHERE owner NOT IN ('SYS', 'SYSTEM', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'WMSYS', 'XDB', 'APEX_030200')
                        UNION ALL
                        SELECT view_name, 'VIEW' as table_type, owner as schema_name
                        FROM all_views
                        WHERE owner NOT IN ('SYS', 'SYSTEM', 'CTXSYS', 'MDSYS', 'OLAPSYS', 'WMSYS', 'XDB', 'APEX_030200')
                        ORDER BY table_name
                    """)
            else:
                if schema:
                    cursor.execute("""
                        SELECT TABLE_NAME, TABLE_TYPE, TABLE_SCHEMA as schema_name
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA = ?
                        ORDER BY TABLE_NAME
                    """, (schema,))
                else:
                    cursor.execute("""
                        SELECT TABLE_NAME, TABLE_TYPE, TABLE_SCHEMA as schema_name
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_SCHEMA NOT IN ('sys', 'information_schema')
                        ORDER BY TABLE_NAME
                    """)
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    'table_name': row[0],
                    'table_type': row[1],
                    'schema_name': row[2]
                })
            
            cursor.close()
            return results
        except Exception as e:
            logger.error(f"Failed to get tables and views: {e}")
            return []
    
    def get_table_schema(self, table_name: str, schema: str = None) -> Optional[TableInfo]:
        if not self.validate_connection():
            return None
        
        try:
            cursor = self.connection.cursor()
            columns = []
            
            if self.db_type == 'oracle':
                # Get column information
                cursor.execute("""
                    SELECT 
                        column_name,
                        data_type,
                        nullable,
                        data_length,
                        data_precision,
                        data_scale,
                        data_default
                    FROM all_tab_columns
                    WHERE table_name = :table_name
                    AND owner = :schema
                    ORDER BY column_id
                """, {'table_name': table_name, 'schema': schema})
                
                for row in cursor.fetchall():
                    columns.append(ColumnInfo(
                        column_name=row[0],
                        data_type=row[1],
                        nullable=(row[2] == 'Y'),
                        max_length=row[3],
                        precision=row[4],
                        scale=row[5],
                        default_value=row[6]
                    ))
                
                # Get primary key information
                cursor.execute("""
                    SELECT column_name
                    FROM all_cons_columns
                    WHERE table_name = :table_name
                    AND owner = :schema
                    AND constraint_name IN (
                        SELECT constraint_name
                        FROM all_constraints
                        WHERE table_name = :table_name
                        AND owner = :schema
                        AND constraint_type = 'P'
                    )
                    ORDER BY position
                """, {'table_name': table_name, 'schema': schema})
                
                primary_keys = [row[0] for row in cursor.fetchall()]
                
                # Get unique constraints
                cursor.execute("""
                    SELECT constraint_name, column_name
                    FROM all_cons_columns
                    WHERE table_name = :table_name
                    AND owner = :schema
                    AND constraint_name IN (
                        SELECT constraint_name
                        FROM all_constraints
                        WHERE table_name = :table_name
                        AND owner = :schema
                        AND constraint_type = 'U'
                    )
                    ORDER BY constraint_name, position
                """, {'table_name': table_name, 'schema': schema})
                
            else:
                # SQL Server column information
                cursor.execute("""
                    SELECT 
                        c.COLUMN_NAME,
                        c.DATA_TYPE,
                        c.IS_NULLABLE,
                        c.CHARACTER_MAXIMUM_LENGTH,
                        c.NUMERIC_PRECISION,
                        c.NUMERIC_SCALE,
                        c.COLUMN_DEFAULT
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    WHERE c.TABLE_NAME = ?
                    AND c.TABLE_SCHEMA = ?
                    ORDER BY c.ORDINAL_POSITION
                """, (table_name, schema))
                
                for row in cursor.fetchall():
                    columns.append(ColumnInfo(
                        column_name=row[0],
                        data_type=row[1],
                        nullable=(row[2] == 'YES'),
                        max_length=row[3],
                        precision=row[4],
                        scale=row[5],
                        default_value=row[6]
                    ))
                
                # Get primary key information
                cursor.execute("""
                    SELECT c.COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE c
                    INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                        ON c.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                        AND c.TABLE_SCHEMA = tc.TABLE_SCHEMA
                    WHERE tc.TABLE_NAME = ?
                    AND tc.TABLE_SCHEMA = ?
                    AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                    ORDER BY c.ORDINAL_POSITION
                """, (table_name, schema))
                
                primary_keys = [row[0] for row in cursor.fetchall()]
            
            # Mark primary key columns
            for col in columns:
                if col.column_name in primary_keys:
                    col.is_primary_key = True
            
            # Get row count estimate
            row_count = self._get_row_count(table_name, schema)
            
            # Determine table type
            table_type = self._get_table_type(table_name, schema)
            
            cursor.close()
            
            return TableInfo(
                table_name=table_name,
                schema_name=schema,
                table_type=table_type,
                columns=columns,
                primary_keys=primary_keys,
                unique_constraints=[],
                row_count=row_count
            )
            
        except Exception as e:
            logger.error(f"Failed to get table schema for {schema}.{table_name}: {e}")
            return None
    
    def get_sample_data(self, table_name: str, schema: str = None, rows: int = 10) -> Optional[pd.DataFrame]:
        if not self.validate_connection():
            return None
        
        try:
            if self.db_type == 'oracle':
                query = f"SELECT * FROM {schema}.{table_name} WHERE ROWNUM <= {rows}"
            else:
                query = f"SELECT TOP {rows} * FROM {schema}.{table_name}"
            
            df = pd.read_sql(query, self.connection)
            return df
            
        except Exception as e:
            logger.error(f"Failed to get sample data for {schema}.{table_name}: {e}")
            return None
    
    def _get_row_count(self, table_name: str, schema: str = None) -> Optional[int]:
        try:
            cursor = self.connection.cursor()
            
            if self.db_type == 'oracle':
                cursor.execute("""
                    SELECT num_rows
                    FROM all_tables
                    WHERE table_name = :table_name
                    AND owner = :schema
                """, {'table_name': table_name, 'schema': schema})
            else:
                cursor.execute("""
                    SELECT SUM(rows)
                    FROM sys.partitions
                    WHERE object_id = OBJECT_ID(?)
                    AND index_id IN (0, 1)
                """, (f"{schema}.{table_name}",))
            
            result = cursor.fetchone()
            cursor.close()
            
            return result[0] if result and result[0] else None
            
        except Exception as e:
            logger.error(f"Failed to get row count for {schema}.{table_name}: {e}")
            return None
    
    def _get_table_type(self, table_name: str, schema: str = None) -> str:
        try:
            cursor = self.connection.cursor()
            
            if self.db_type == 'oracle':
                cursor.execute("""
                    SELECT 'TABLE' as table_type
                    FROM all_tables
                    WHERE table_name = :table_name
                    AND owner = :schema
                    UNION ALL
                    SELECT 'VIEW' as table_type
                    FROM all_views
                    WHERE view_name = :table_name
                    AND owner = :schema
                """, {'table_name': table_name, 'schema': schema})
            else:
                cursor.execute("""
                    SELECT TABLE_TYPE
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME = ?
                    AND TABLE_SCHEMA = ?
                """, (table_name, schema))
            
            result = cursor.fetchone()
            cursor.close()
            
            return result[0] if result else 'TABLE'
            
        except Exception as e:
            logger.error(f"Failed to get table type for {schema}.{table_name}: {e}")
            return 'TABLE'
    
    def close(self):
        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Database connection closed")
    
    def __del__(self):
        self.close()