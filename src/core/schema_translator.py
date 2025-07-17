from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
import re
from loguru import logger

from .database_connector import TableInfo, ColumnInfo


@dataclass
class DDLOutput:
    table_ddl: str
    view_ddl: Optional[str]
    target_table_name: str
    target_view_name: Optional[str]
    files_to_create: List[Tuple[str, str]]  # (file_path, content)


class SchemaTranslator:
    def __init__(self):
        self.oracle_to_sqlserver_mapping = {
            'VARCHAR2': 'NVARCHAR',
            'NVARCHAR2': 'NVARCHAR',
            'CHAR': 'NCHAR',
            'NCHAR': 'NCHAR',
            'NUMBER': 'DECIMAL',
            'FLOAT': 'FLOAT',
            'BINARY_FLOAT': 'REAL',
            'BINARY_DOUBLE': 'FLOAT',
            'DATE': 'DATETIME2',
            'TIMESTAMP': 'DATETIME2',
            'TIMESTAMP WITH TIME ZONE': 'DATETIMEOFFSET',
            'TIMESTAMP WITH LOCAL TIME ZONE': 'DATETIME2',
            'CLOB': 'NVARCHAR(MAX)',
            'NCLOB': 'NVARCHAR(MAX)',
            'BLOB': 'VARBINARY(MAX)',
            'RAW': 'VARBINARY',
            'LONG RAW': 'VARBINARY(MAX)',
            'ROWID': 'UNIQUEIDENTIFIER',
            'UROWID': 'UNIQUEIDENTIFIER',
            'XMLTYPE': 'XML',
            'BFILE': 'NVARCHAR(MAX)',
            'LONG': 'NVARCHAR(MAX)'
        }
        
        # Special handling for NUMBER without precision/scale
        self.number_default_mapping = 'BIGINT'
    
    def translate_oracle_to_sqlserver(self, table_info: TableInfo) -> TableInfo:
        translated_columns = []
        
        for col in table_info.columns:
            translated_col = self._translate_column(col)
            translated_columns.append(translated_col)
        
        return TableInfo(
            table_name=table_info.table_name,
            schema_name=table_info.schema_name,
            table_type=table_info.table_type,
            columns=translated_columns,
            primary_keys=table_info.primary_keys,
            unique_constraints=table_info.unique_constraints,
            row_count=table_info.row_count
        )
    
    def _translate_column(self, col: ColumnInfo) -> ColumnInfo:
        original_type = col.data_type.upper()
        
        # Handle NUMBER type with special logic
        if original_type == 'NUMBER':
            if col.precision is None and col.scale is None:
                # NUMBER without precision/scale -> BIGINT
                new_type = self.number_default_mapping
                new_precision = None
                new_scale = None
            elif col.scale is None or col.scale == 0:
                # NUMBER(n) or NUMBER(n,0) -> INT or BIGINT
                if col.precision and col.precision <= 9:
                    new_type = 'INT'
                else:
                    new_type = 'BIGINT'
                new_precision = None
                new_scale = None
            else:
                # NUMBER(n,s) -> DECIMAL(n,s)
                new_type = 'DECIMAL'
                new_precision = col.precision
                new_scale = col.scale
        else:
            # Use mapping table for other types
            new_type = self.oracle_to_sqlserver_mapping.get(original_type, original_type)
            new_precision = col.precision
            new_scale = col.scale
        
        # Handle length for string types
        new_length = col.max_length
        if new_type in ['NVARCHAR', 'NCHAR', 'VARBINARY']:
            if new_length is None:
                if new_type == 'NVARCHAR':
                    new_length = 255  # Default for NVARCHAR
                elif new_type == 'NCHAR':
                    new_length = 1    # Default for NCHAR
            
            # Convert Oracle length to SQL Server length
            if new_type in ['NVARCHAR', 'NCHAR'] and new_length:
                # Oracle stores byte length, SQL Server stores character length
                # For Unicode, divide by 4 (Oracle UTF-8 can be up to 4 bytes per char)
                # Then cap at SQL Server limit
                new_length = new_length // 4  # Convert from byte length to character length
                new_length = max(1, min(new_length, 4000))  # Ensure at least 1, max 4000
            elif new_type == 'VARBINARY' and new_length:
                # VARBINARY lengths should be preserved as-is (byte lengths)
                new_length = min(new_length, 8000)  # SQL Server VARBINARY max without MAX
        
        return ColumnInfo(
            column_name=col.column_name,
            data_type=new_type,
            nullable=col.nullable,
            max_length=new_length,
            precision=new_precision,
            scale=new_scale,
            is_primary_key=col.is_primary_key,
            is_unique=col.is_unique,
            default_value=col.default_value
        )
    
    def apply_naming_conventions(self, table_name: str, source_prefix: str) -> str:
        # Clean and format table name
        clean_table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name.lower())
        clean_prefix = re.sub(r'[^a-zA-Z0-9_]', '_', source_prefix.lower())
        
        # Apply naming convention: {source_prefix}_{table_name}
        return f"{clean_prefix}_{clean_table_name}"
    
    def add_audit_columns(self, table_info: TableInfo) -> TableInfo:
        # Add standard audit column
        audit_column = ColumnInfo(
            column_name='record_insert_datetime',
            data_type='DATETIME2',
            nullable=False,
            default_value='GETDATE()'
        )
        
        # Create new column list with audit column
        new_columns = table_info.columns + [audit_column]
        
        return TableInfo(
            table_name=table_info.table_name,
            schema_name=table_info.schema_name,
            table_type=table_info.table_type,
            columns=new_columns,
            primary_keys=table_info.primary_keys,
            unique_constraints=table_info.unique_constraints,
            row_count=table_info.row_count
        )
    
    def generate_ddl(self, table_info: TableInfo, target_schema: str, 
                    target_table_name: str, create_view: bool = False) -> DDLOutput:
        
        # Generate table DDL
        table_ddl = self._generate_table_ddl(table_info, target_schema, target_table_name)
        
        # Generate view DDL if requested
        view_ddl = None
        target_view_name = None
        if create_view:
            target_view_name = f"{table_info.schema_name}_{table_info.table_name}"
            view_ddl = self._generate_view_ddl(target_schema, target_table_name, 
                                             target_view_name, table_info.columns)
        
        # Determine file paths
        files_to_create = []
        
        # Table file
        table_file_path = f"{target_schema}/Tables/{target_table_name}.sql"
        files_to_create.append((table_file_path, table_ddl))
        
        # View file
        if view_ddl:
            view_file_path = f"{target_schema}/Views/{target_view_name}.sql"
            files_to_create.append((view_file_path, view_ddl))
        
        return DDLOutput(
            table_ddl=table_ddl,
            view_ddl=view_ddl,
            target_table_name=target_table_name,
            target_view_name=target_view_name,
            files_to_create=files_to_create
        )
    
    def _generate_table_ddl(self, table_info: TableInfo, target_schema: str, 
                           target_table_name: str) -> str:
        ddl_parts = []
        
        # Table header
        ddl_parts.append(f"CREATE TABLE [{target_schema}].[{target_table_name}] (")
        
        # Columns
        column_definitions = []
        for col in table_info.columns:
            col_def = self._format_column_definition(col)
            column_definitions.append(f"    {col_def}")
        
        ddl_parts.append(",\n".join(column_definitions))
        
        # Primary key constraint
        if table_info.primary_keys:
            pk_columns = ", ".join(f"[{pk}]" for pk in table_info.primary_keys)
            constraint_name = f"PK_{target_table_name}"
            ddl_parts.append(f",\n    CONSTRAINT [{constraint_name}] PRIMARY KEY ({pk_columns})")
        
        # Table footer
        ddl_parts.append("\n);")
        
        return "".join(ddl_parts)
    
    def _format_column_definition(self, col: ColumnInfo) -> str:
        parts = [f"[{col.column_name}]"]
        
        # Data type with length/precision
        if col.data_type in ['NVARCHAR', 'NCHAR', 'VARBINARY']:
            if col.max_length:
                if col.max_length > 4000:
                    parts.append(f"{col.data_type}(MAX)")
                else:
                    parts.append(f"{col.data_type}({col.max_length})")
            else:
                parts.append(f"{col.data_type}(255)")  # Default length
        elif col.data_type == 'DECIMAL' and col.precision:
            if col.scale:
                parts.append(f"{col.data_type}({col.precision},{col.scale})")
            else:
                parts.append(f"{col.data_type}({col.precision})")
        else:
            parts.append(col.data_type)
        
        # Nullable
        if not col.nullable:
            parts.append("NOT NULL")
        
        # Default value
        if col.default_value:
            if col.column_name == 'record_insert_datetime':
                parts.append(f"CONSTRAINT [DF_{col.column_name}] DEFAULT ({col.default_value})")
            else:
                parts.append(f"DEFAULT {col.default_value}")
        
        return " ".join(parts)
    
    def _generate_view_ddl(self, target_schema: str, target_table_name: str, 
                          view_name: str, columns: List[ColumnInfo]) -> str:
        ddl_parts = []
        
        # View header
        ddl_parts.append(f"CREATE VIEW [{target_schema}].[{view_name}] AS")
        
        # Select statement
        column_list = []
        for col in columns:
            if col.column_name != 'record_insert_datetime':  # Exclude audit column from view
                column_list.append(f"    [{col.column_name}]")
        
        ddl_parts.append("SELECT")
        ddl_parts.append(",\n".join(column_list))
        ddl_parts.append(f"FROM [{target_schema}].[{target_table_name}];")
        
        return "\n".join(ddl_parts)
    
    def generate_sqlproj_entries(self, files_to_create: List[Tuple[str, str]]) -> List[str]:
        entries = []
        
        for file_path, _ in files_to_create:
            # Convert file path to proper format for .sqlproj
            clean_path = file_path.replace('/', '\\')
            
            if 'Tables' in file_path:
                entries.append(f'    <Build Include="{clean_path}" />')
            elif 'Views' in file_path:
                entries.append(f'    <Build Include="{clean_path}" />')
        
        return entries
    
    def validate_naming_convention(self, table_name: str) -> Tuple[bool, str]:
        # Check if table name follows SQL Server naming rules
        if not table_name:
            return False, "Table name cannot be empty"
        
        if len(table_name) > 128:
            return False, "Table name cannot exceed 128 characters"
        
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', table_name):
            return False, "Table name must start with letter or underscore and contain only letters, numbers, and underscores"
        
        # Check for reserved words (basic list)
        reserved_words = {
            'SELECT', 'FROM', 'WHERE', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP',
            'ALTER', 'TABLE', 'VIEW', 'INDEX', 'DATABASE', 'SCHEMA', 'USER', 'ORDER',
            'GROUP', 'HAVING', 'UNION', 'JOIN', 'INNER', 'LEFT', 'RIGHT', 'FULL',
            'OUTER', 'ON', 'AS', 'AND', 'OR', 'NOT', 'IN', 'EXISTS', 'LIKE', 'BETWEEN'
        }
        
        if table_name.upper() in reserved_words:
            return False, f"'{table_name}' is a reserved word and cannot be used as a table name"
        
        return True, "Valid table name"
    
    def get_data_type_mapping_explanation(self, oracle_type: str) -> str:
        oracle_type_upper = oracle_type.upper()
        
        explanations = {
            'VARCHAR2': 'Mapped to NVARCHAR for Unicode support',
            'NUMBER': 'Mapped to DECIMAL for precision, or BIGINT if no precision specified',
            'DATE': 'Mapped to DATETIME2 for better precision and range',
            'TIMESTAMP': 'Mapped to DATETIME2 for SQL Server compatibility',
            'CLOB': 'Mapped to NVARCHAR(MAX) for large text storage',
            'BLOB': 'Mapped to VARBINARY(MAX) for large binary storage',
            'RAW': 'Mapped to VARBINARY for binary data',
            'ROWID': 'Mapped to UNIQUEIDENTIFIER as closest equivalent'
        }
        
        return explanations.get(oracle_type_upper, f"Direct mapping from {oracle_type}")
    
    def estimate_storage_impact(self, table_info: TableInfo) -> Dict[str, any]:
        # Estimate storage requirements and performance impact
        estimated_row_size = 0
        variable_length_columns = 0
        
        for col in table_info.columns:
            if col.data_type in ['NVARCHAR', 'VARBINARY']:
                if col.max_length:
                    estimated_row_size += col.max_length * 2  # Unicode factor
                else:
                    estimated_row_size += 510  # Default estimate
                variable_length_columns += 1
            elif col.data_type == 'NCHAR':
                estimated_row_size += (col.max_length or 1) * 2
            elif col.data_type in ['INT', 'BIGINT']:
                estimated_row_size += 8
            elif col.data_type == 'DECIMAL':
                estimated_row_size += 9  # Approximate for DECIMAL
            elif col.data_type == 'DATETIME2':
                estimated_row_size += 8
            else:
                estimated_row_size += 8  # Default estimate
        
        # Add overhead for row structure
        estimated_row_size += 24  # Row overhead
        
        # Calculate estimated table size
        estimated_table_size_mb = 0
        if table_info.row_count:
            estimated_table_size_mb = (estimated_row_size * table_info.row_count) / (1024 * 1024)
        
        return {
            'estimated_row_size_bytes': estimated_row_size,
            'estimated_table_size_mb': estimated_table_size_mb,
            'variable_length_columns': variable_length_columns,
            'has_large_objects': any(col.data_type.endswith('(MAX)') for col in table_info.columns),
            'performance_notes': self._get_performance_notes(table_info)
        }
    
    def _get_performance_notes(self, table_info: TableInfo) -> List[str]:
        notes = []
        
        # Check for potential performance issues
        has_clob = any('MAX' in col.data_type for col in table_info.columns)
        if has_clob:
            notes.append("Table contains large object columns (MAX) which may impact performance")
        
        if not table_info.primary_keys:
            notes.append("Table has no primary key - consider adding one for better performance")
        
        varchar_columns = [col for col in table_info.columns if col.data_type == 'NVARCHAR']
        if len(varchar_columns) > 10:
            notes.append("Table has many variable-length columns which may impact row storage")
        
        return notes