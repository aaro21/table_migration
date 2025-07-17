import os
from pathlib import Path
from typing import Dict, Any, Optional
from dotenv import load_dotenv
import json
from loguru import logger


def load_config() -> Dict[str, Any]:
    # Load environment variables from .env file
    env_path = Path.cwd() / '.env'
    if env_path.exists():
        load_dotenv(env_path)
    
    config = {
        # Database configuration
        'oracle': {
            'dsn': os.getenv('ORACLE_DSN', ''),
            'username': os.getenv('ORACLE_USERNAME', ''),
            'password': os.getenv('ORACLE_PASSWORD', '')
        },
        'source_sqlserver': {
            'host': os.getenv('SOURCE_SQL_SERVER_HOST', ''),
            'database': os.getenv('SOURCE_SQL_SERVER_DATABASE', ''),
            'trusted_connection': os.getenv('SOURCE_SQL_SERVER_TRUSTED_CONNECTION', 'yes').lower() == 'yes',
            'username': os.getenv('SOURCE_SQL_SERVER_USERNAME', ''),
            'password': os.getenv('SOURCE_SQL_SERVER_PASSWORD', '')
        },
        'temp_sqlserver': {
            'host': os.getenv('TEMP_SQL_SERVER_HOST', ''),
            'database': os.getenv('TEMP_SQL_SERVER_DATABASE', ''),
            'trusted_connection': os.getenv('TEMP_SQL_SERVER_TRUSTED_CONNECTION', 'yes').lower() == 'yes',
            'username': os.getenv('TEMP_SQL_SERVER_USERNAME', ''),
            'password': os.getenv('TEMP_SQL_SERVER_PASSWORD', '')
        },
        'bronze_sqlserver': {
            'host': os.getenv('BRONZE_SQL_SERVER_HOST', ''),
            'database': os.getenv('BRONZE_SQL_SERVER_DATABASE', ''),
            'trusted_connection': os.getenv('BRONZE_SQL_SERVER_TRUSTED_CONNECTION', 'yes').lower() == 'yes',
            'username': os.getenv('BRONZE_SQL_SERVER_USERNAME', ''),
            'password': os.getenv('BRONZE_SQL_SERVER_PASSWORD', '')
        },
        
        # AI configuration
        'ai': {
            'api_key': os.getenv('OPENAI_API_KEY', ''),
            'base_url': os.getenv('OPENAI_BASE_URL', 'https://api.openai.com/v1'),
            'model': os.getenv('OPENAI_MODEL', 'gpt-3.5-turbo')
        },
        
        # Git configuration
        'git': {
            'default_base_branch': os.getenv('DEFAULT_BASE_BRANCH', 'dev'),
            'author_name': os.getenv('GIT_AUTHOR_NAME', 'Data Migration Tool'),
            'author_email': os.getenv('GIT_AUTHOR_EMAIL', 'data-migration@company.com')
        },
        
        # Logging configuration
        'logging': {
            'level': os.getenv('LOG_LEVEL', 'INFO'),
            'database_connection': os.getenv('LOG_DATABASE_CONNECTION', '')
        },
        
        # Application settings
        'app': {
            'default_source_prefix': os.getenv('DEFAULT_SOURCE_PREFIX', 'src'),
            'default_target_database': os.getenv('DEFAULT_TARGET_DATABASE', 'DataWarehouse'),
            'default_target_schema': os.getenv('DEFAULT_TARGET_SCHEMA', 'temp_schema')
        }
    }
    
    return config


def load_user_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    if config_path is None:
        config_path = Path.home() / '.data_migration_tool' / 'config.json'
    else:
        config_path = Path(config_path)
    
    if not config_path.exists():
        logger.info(f"No user config found at {config_path}")
        return {}
    
    try:
        with open(config_path, 'r') as f:
            user_config = json.load(f)
        logger.info(f"Loaded user config from {config_path}")
        return user_config
    except Exception as e:
        logger.error(f"Failed to load user config: {e}")
        return {}


def save_user_config(config: Dict[str, Any], config_path: Optional[str] = None):
    if config_path is None:
        config_path = Path.home() / '.data_migration_tool' / 'config.json'
    else:
        config_path = Path(config_path)
    
    # Create directory if it doesn't exist
    config_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)
        logger.info(f"Saved user config to {config_path}")
    except Exception as e:
        logger.error(f"Failed to save user config: {e}")


def get_database_templates() -> Dict[str, Dict[str, Any]]:
    return {
        'oracle_local': {
            'type': 'oracle',
            'dsn': 'localhost:1521/xe',
            'description': 'Local Oracle XE database'
        },
        'sqlserver_local': {
            'type': 'sqlserver',
            'host': 'localhost',
            'database': 'master',
            'trusted_connection': True,
            'description': 'Local SQL Server with trusted connection'
        },
        'sqlserver_auth': {
            'type': 'sqlserver',
            'host': 'your-server.database.windows.net',
            'database': 'your-database',
            'trusted_connection': False,
            'description': 'SQL Server with username/password authentication'
        }
    }


def get_naming_convention_templates() -> Dict[str, Dict[str, Any]]:
    return {
        'standard': {
            'prefix_format': '{source_system}_{table_name}',
            'view_format': '{source_schema}_{table_name}',
            'description': 'Standard naming convention'
        },
        'with_timestamp': {
            'prefix_format': '{source_system}_{table_name}_{timestamp}',
            'view_format': '{source_schema}_{table_name}_view',
            'description': 'Include timestamp in table names'
        },
        'schema_prefixed': {
            'prefix_format': '{source_schema}_{source_system}_{table_name}',
            'view_format': '{source_schema}_{table_name}',
            'description': 'Include source schema in table names'
        }
    }


def validate_config(config: Dict[str, Any]) -> tuple[bool, list[str]]:
    errors = []
    
    # Validate Oracle configuration if provided
    oracle_config = config.get('oracle', {})
    if oracle_config.get('username') and not oracle_config.get('dsn'):
        errors.append("Oracle DSN is required when username is provided")
    
    # Validate SQL Server configuration if provided
    sqlserver_config = config.get('sqlserver', {})
    if sqlserver_config.get('host') and not sqlserver_config.get('database'):
        errors.append("SQL Server database is required when host is provided")
    
    # Validate AI configuration if provided
    ai_config = config.get('ai', {})
    if ai_config.get('base_url') and not ai_config.get('api_key'):
        errors.append("AI API key is required when base URL is provided")
    
    return len(errors) == 0, errors


def get_default_file_locations() -> Dict[str, str]:
    return {
        'config_dir': str(Path.home() / '.data_migration_tool'),
        'log_dir': str(Path.cwd() / 'logs'),
        'temp_dir': str(Path.cwd() / 'temp'),
        'output_dir': str(Path.cwd() / 'output')
    }


def ensure_directories():
    locations = get_default_file_locations()
    
    for location_name, location_path in locations.items():
        path = Path(location_path)
        if not path.exists():
            try:
                path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory: {location_path}")
            except Exception as e:
                logger.error(f"Failed to create directory {location_path}: {e}")


def get_connection_string_templates() -> Dict[str, str]:
    return {
        'oracle_basic': "username/password@host:port/service_name",
        'oracle_sid': "username/password@host:port:sid",
        'sqlserver_trusted': "Server=server_name;Database=database_name;Trusted_Connection=True;",
        'sqlserver_auth': "Server=server_name;Database=database_name;User Id=username;Password=password;",
        'sqlserver_azure': "Server=server_name.database.windows.net;Database=database_name;User Id=username;Password=password;Encrypt=True;"
    }