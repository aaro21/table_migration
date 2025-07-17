# Data Migration Tool

A Streamlit-based application that automates the process of migrating table schemas from Oracle and SQL Server sources to SQL Server warehouse environments (TEMP and Bronze layers).

## Features

- **Multi-Database Support**: Connect to Oracle and SQL Server databases
- **Intelligent Schema Translation**: Automatic data type mapping with AI assistance
- **Git Integration**: Automated branch creation, file management, and commits
- **Streamlit UI**: User-friendly web interface for migration management
- **DDL Generation**: Generate SQL Server DDL with naming conventions
- **Project File Management**: Automatic .sqlproj file updates
- **Comprehensive Logging**: Track all migration activities
- **Docker Support**: Easy deployment and distribution

## Quick Start

### Prerequisites

- Python 3.11+
- Docker (optional)
- Git
- Oracle client libraries (for Oracle connections)
- SQL Server ODBC Driver

### Installation

1. Clone the repository:
```bash
git clone <repository-url>
cd table_migration
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

4. Run the application:
```bash
streamlit run app.py
```

### Docker Installation

1. Build the Docker image:
```bash
docker build -t data-migration-tool .
```

2. Run with Docker Compose:
```bash
docker-compose up
```

## Configuration

### Environment Variables

Create a `.env` file with the following variables:

```env
# Source Database Configuration
# Oracle Source
ORACLE_DSN=your_oracle_dsn_here
ORACLE_USERNAME=your_oracle_username
ORACLE_PASSWORD=your_oracle_password

# SQL Server Source (if migrating from SQL Server)
SOURCE_SQL_SERVER_HOST=your_source_sql_server_host
SOURCE_SQL_SERVER_DATABASE=your_source_database_name
SOURCE_SQL_SERVER_TRUSTED_CONNECTION=yes
SOURCE_SQL_SERVER_USERNAME=your_source_username
SOURCE_SQL_SERVER_PASSWORD=your_source_password

# Destination Database Configuration
# TEMP Database
TEMP_SQL_SERVER_HOST=your_temp_sql_server_host
TEMP_SQL_SERVER_DATABASE=your_temp_database_name
TEMP_SQL_SERVER_TRUSTED_CONNECTION=yes
TEMP_SQL_SERVER_USERNAME=your_temp_username
TEMP_SQL_SERVER_PASSWORD=your_temp_password

# Bronze Database
BRONZE_SQL_SERVER_HOST=your_bronze_sql_server_host
BRONZE_SQL_SERVER_DATABASE=your_bronze_database_name
BRONZE_SQL_SERVER_TRUSTED_CONNECTION=yes
BRONZE_SQL_SERVER_USERNAME=your_bronze_username
BRONZE_SQL_SERVER_PASSWORD=your_bronze_password

# AI Configuration
OPENAI_API_KEY=your_openai_api_key
OPENAI_BASE_URL=https://api.openai.com/v1

# Git Configuration
DEFAULT_BASE_BRANCH=dev
GIT_AUTHOR_NAME=Data Migration Tool
GIT_AUTHOR_EMAIL=data-migration@company.com
```

### Database Connections

#### Oracle Connection
- **DSN Format**: `host:port/service_name` or `host:port:sid`
- **Example**: `localhost:1521/xe`

#### SQL Server Connection
- **Trusted Connection**: Uses Windows Authentication
- **Username/Password**: Standard SQL Server authentication
- **Example**: `Server=localhost;Database=master;Trusted_Connection=True;`

## Usage

### Enhanced Migration Workflow

1. **Connect to Source Database**
   - Select Oracle or SQL Server as source
   - Enter source connection details
   - Test source connection

2. **Connect to Destination Databases**
   - Configure TEMP database connection
   - Configure Bronze database connection
   - Test destination connections

3. **Configure Git Settings**
   - Set repository path
   - Specify base branch
   - Validate Git setup

4. **Select Source Object**
   - Browse available schemas
   - Search and filter tables/views
   - Preview table structure

5. **Configure Migration Settings**
   - Set source system prefix
   - Choose target database (TEMP or Bronze)
   - Select target schema
   - Configure view creation options

6. **Generate DDL**
   - Generate SQL Server DDL with data type mapping
   - Preview generated files
   - Review storage impact analysis

7. **Execute Migration**
   - Create feature branch
   - Generate DDL files in correct database project
   - Update .sqlproj files automatically
   - Commit changes with detailed messages

### Data Type Mapping

The tool automatically maps Oracle data types to SQL Server equivalents:

| Oracle Type | SQL Server Type | Notes |
|-------------|----------------|-------|
| VARCHAR2(n) | NVARCHAR(n) | Unicode support |
| NUMBER(p,s) | DECIMAL(p,s) | Precision preserved |
| NUMBER | BIGINT | No precision specified |
| DATE | DATETIME2 | Better precision |
| TIMESTAMP | DATETIME2 | SQL Server compatible |
| CLOB | NVARCHAR(MAX) | Large text |
| BLOB | VARBINARY(MAX) | Large binary |

### Naming Conventions

- **Table Names**: `{source_prefix}_{table_name}`
- **View Names**: `{source_schema}_{table_name}`
- **Audit Columns**: `record_insert_datetime` added automatically

### File Structure

Generated files follow this structure:
```
DatabaseName/
├── DatabaseName.sqlproj
├── temp_schema/
│   ├── Tables/
│   │   └── sourcesystem_tablename.sql
│   └── Views/
│       └── sourcesystem_tablename.sql
└── bronze_schema/
    ├── Tables/
    │   └── sourcesystem_tablename.sql
    └── Views/
        └── sourcesystem_view_name.sql
```

## Advanced Features

### AI-Assisted Data Type Mapping

The tool can use AI to suggest optimal data type mappings:

1. Configure OpenAI API key
2. Enable AI suggestions in the UI
3. Review AI recommendations
4. Accept or override suggestions

### Migration Logging

All migrations are logged with:
- User information
- Source and target details
- Success/failure status
- File paths created
- Error messages

### Performance Analysis

The tool provides storage impact analysis:
- Estimated row size
- Table size projections
- Performance recommendations
- Large object detection

## Troubleshooting

### Common Issues

1. **Oracle Connection Failed**
   - Verify Oracle client libraries are installed
   - Check DSN format and network connectivity
   - Ensure proper credentials

2. **SQL Server Connection Failed**
   - Verify ODBC driver is installed
   - Check server accessibility
   - Validate authentication method

3. **Git Operations Failed**
   - Ensure Git repository is initialized
   - Check branch permissions
   - Verify working directory is clean

4. **DDL Generation Failed**
   - Check table accessibility
   - Verify schema permissions
   - Review data type compatibility

### Logging

Check logs in the `logs/` directory:
- `migration_tool_YYYY-MM-DD.log`: Daily application logs
- Console output for real-time debugging

## Development

### Project Structure

```
src/
├── core/
│   ├── database_connector.py    # Database connectivity
│   ├── schema_translator.py     # Data type mapping and DDL generation
│   ├── git_manager.py          # Git operations
│   └── ai_assistant.py         # AI integration
├── ui/
│   └── main_ui.py              # Streamlit interface
└── utils/
    ├── config.py               # Configuration management
    └── logging_helper.py       # Logging utilities
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Submit a pull request

### Testing

Run tests with:
```bash
pytest tests/
```

## License

[Add your license information here]

## Support

For issues and feature requests:
- Create an issue in the repository
- Check the troubleshooting guide
- Review existing documentation

## Roadmap

- [ ] Batch migration support
- [ ] Advanced data validation
- [ ] Custom data type mapping rules
- [ ] Integration with CI/CD pipelines
- [ ] Enhanced performance optimization
- [ ] Multi-tenant support# table_migration
