import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Optional
import pyodbc
from loguru import logger


def setup_logging(log_level: str = "INFO", log_to_database: bool = False, database_connection: Optional[str] = None):
    # Remove default logger
    logger.remove()
    
    # Create logs directory
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Console logging
    logger.add(
        sys.stdout,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        level=log_level,
        colorize=True
    )
    
    # File logging
    log_file = log_dir / "migration_tool_{time:YYYY-MM-DD}.log"
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
        level=log_level,
        rotation="1 day",
        retention="30 days",
        compression="zip"
    )
    
    # Database logging if configured
    if log_to_database and database_connection:
        try:
            setup_database_logging(database_connection)
        except Exception as e:
            logger.error(f"Failed to setup database logging: {e}")


def setup_database_logging(connection_string: str):
    def database_sink(message):
        try:
            record = message.record
            
            # Parse log level and message
            level = record['level'].name
            message_text = record['message']
            timestamp = record['time']
            module_name = record['name']
            function_name = record['function']
            line_number = record['line']
            
            # Connect to database and insert log
            with pyodbc.connect(connection_string) as conn:
                cursor = conn.cursor()
                
                # Ensure the log table exists
                create_log_table_if_not_exists(cursor)
                
                # Insert log record
                cursor.execute("""
                    INSERT INTO migration_tool_logs 
                    (timestamp, level, module_name, function_name, line_number, message)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (timestamp, level, module_name, function_name, line_number, message_text))
                
                conn.commit()
                
        except Exception as e:
            # Don't let logging errors break the application
            print(f"Database logging failed: {e}")
    
    logger.add(database_sink, level="INFO")


def create_log_table_if_not_exists(cursor):
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='migration_tool_logs' AND xtype='U')
        BEGIN
            CREATE TABLE migration_tool_logs (
                log_id INT IDENTITY(1,1) PRIMARY KEY,
                timestamp DATETIME2 NOT NULL,
                level NVARCHAR(10) NOT NULL,
                module_name NVARCHAR(100),
                function_name NVARCHAR(100),
                line_number INT,
                message NVARCHAR(MAX),
                created_at DATETIME2 NOT NULL DEFAULT GETDATE()
            );
            
            CREATE INDEX IX_migration_tool_logs_timestamp ON migration_tool_logs(timestamp);
            CREATE INDEX IX_migration_tool_logs_level ON migration_tool_logs(level);
        END
    """)


def log_migration_start(user_name: str, source_system: str, source_schema: str, 
                       source_table: str, target_database: str, target_schema: str, 
                       target_table: str, git_branch: str):
    logger.info(f"Migration started", extra={
        'user_name': user_name,
        'source_system': source_system,
        'source_schema': source_schema,
        'source_table': source_table,
        'target_database': target_database,
        'target_schema': target_schema,
        'target_table': target_table,
        'git_branch': git_branch,
        'event_type': 'migration_start'
    })


def log_migration_success(user_name: str, source_system: str, source_schema: str, 
                         source_table: str, target_database: str, target_schema: str, 
                         target_table: str, git_branch: str, files_created: list):
    logger.info(f"Migration completed successfully", extra={
        'user_name': user_name,
        'source_system': source_system,
        'source_schema': source_schema,
        'source_table': source_table,
        'target_database': target_database,
        'target_schema': target_schema,
        'target_table': target_table,
        'git_branch': git_branch,
        'files_created': files_created,
        'event_type': 'migration_success'
    })


def log_migration_failure(user_name: str, source_system: str, source_schema: str, 
                         source_table: str, target_database: str, target_schema: str, 
                         target_table: str, git_branch: str, error_message: str):
    logger.error(f"Migration failed: {error_message}", extra={
        'user_name': user_name,
        'source_system': source_system,
        'source_schema': source_schema,
        'source_table': source_table,
        'target_database': target_database,
        'target_schema': target_schema,
        'target_table': target_table,
        'git_branch': git_branch,
        'error_message': error_message,
        'event_type': 'migration_failure'
    })


def log_ddl_generation(table_name: str, schema_name: str, ddl_type: str, success: bool, 
                      error_message: Optional[str] = None):
    if success:
        logger.info(f"DDL generated successfully for {schema_name}.{table_name}", extra={
            'table_name': table_name,
            'schema_name': schema_name,
            'ddl_type': ddl_type,
            'event_type': 'ddl_generation_success'
        })
    else:
        logger.error(f"DDL generation failed for {schema_name}.{table_name}: {error_message}", extra={
            'table_name': table_name,
            'schema_name': schema_name,
            'ddl_type': ddl_type,
            'error_message': error_message,
            'event_type': 'ddl_generation_failure'
        })


def log_database_connection(db_type: str, connection_info: str, success: bool, 
                          error_message: Optional[str] = None):
    if success:
        logger.info(f"Successfully connected to {db_type} database", extra={
            'db_type': db_type,
            'connection_info': connection_info,
            'event_type': 'database_connection_success'
        })
    else:
        logger.error(f"Failed to connect to {db_type} database: {error_message}", extra={
            'db_type': db_type,
            'connection_info': connection_info,
            'error_message': error_message,
            'event_type': 'database_connection_failure'
        })


def log_git_operation(operation: str, branch_name: str, success: bool, 
                     error_message: Optional[str] = None):
    if success:
        logger.info(f"Git operation '{operation}' completed successfully", extra={
            'operation': operation,
            'branch_name': branch_name,
            'event_type': 'git_operation_success'
        })
    else:
        logger.error(f"Git operation '{operation}' failed: {error_message}", extra={
            'operation': operation,
            'branch_name': branch_name,
            'error_message': error_message,
            'event_type': 'git_operation_failure'
        })


def log_file_operation(operation: str, file_path: str, success: bool, 
                      error_message: Optional[str] = None):
    if success:
        logger.info(f"File operation '{operation}' completed: {file_path}", extra={
            'operation': operation,
            'file_path': file_path,
            'event_type': 'file_operation_success'
        })
    else:
        logger.error(f"File operation '{operation}' failed for {file_path}: {error_message}", extra={
            'operation': operation,
            'file_path': file_path,
            'error_message': error_message,
            'event_type': 'file_operation_failure'
        })


def log_data_type_mapping(original_type: str, mapped_type: str, table_name: str, 
                         column_name: str, mapping_reason: Optional[str] = None):
    logger.info(f"Data type mapping: {original_type} -> {mapped_type}", extra={
        'original_type': original_type,
        'mapped_type': mapped_type,
        'table_name': table_name,
        'column_name': column_name,
        'mapping_reason': mapping_reason,
        'event_type': 'data_type_mapping'
    })


def log_performance_metric(metric_name: str, metric_value: float, unit: str, 
                          context: Optional[str] = None):
    logger.info(f"Performance metric: {metric_name} = {metric_value} {unit}", extra={
        'metric_name': metric_name,
        'metric_value': metric_value,
        'unit': unit,
        'context': context,
        'event_type': 'performance_metric'
    })


def log_user_action(action: str, user_name: str, details: Optional[dict] = None):
    logger.info(f"User action: {action}", extra={
        'action': action,
        'user_name': user_name,
        'details': details,
        'event_type': 'user_action'
    })


class MigrationLogger:
    def __init__(self, user_name: str, source_system: str, source_schema: str, 
                 source_table: str, target_database: str, target_schema: str, 
                 target_table: str, git_branch: str):
        self.user_name = user_name
        self.source_system = source_system
        self.source_schema = source_schema
        self.source_table = source_table
        self.target_database = target_database
        self.target_schema = target_schema
        self.target_table = target_table
        self.git_branch = git_branch
        self.start_time = datetime.now()
    
    def log_start(self):
        log_migration_start(
            self.user_name, self.source_system, self.source_schema,
            self.source_table, self.target_database, self.target_schema,
            self.target_table, self.git_branch
        )
    
    def log_success(self, files_created: list):
        duration = datetime.now() - self.start_time
        log_performance_metric("migration_duration", duration.total_seconds(), "seconds", 
                              f"Migration of {self.source_schema}.{self.source_table}")
        
        log_migration_success(
            self.user_name, self.source_system, self.source_schema,
            self.source_table, self.target_database, self.target_schema,
            self.target_table, self.git_branch, files_created
        )
    
    def log_failure(self, error_message: str):
        duration = datetime.now() - self.start_time
        log_performance_metric("migration_duration", duration.total_seconds(), "seconds", 
                              f"Failed migration of {self.source_schema}.{self.source_table}")
        
        log_migration_failure(
            self.user_name, self.source_system, self.source_schema,
            self.source_table, self.target_database, self.target_schema,
            self.target_table, self.git_branch, error_message
        )