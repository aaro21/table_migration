import streamlit as st
import pandas as pd
from typing import Optional, Dict, Any
import os
from pathlib import Path

from core.database_connector import DatabaseConnector, DatabaseManager, TableInfo
from core.schema_translator import SchemaTranslator
from core.git_manager import GitManager
from utils.config import load_config
from utils.logging_helper import setup_logging


def main():
    st.set_page_config(
        page_title="Data Migration Tool",
        page_icon="🔄",
        layout="wide",
        initial_sidebar_state="expanded"
    )
    
    # Initialize session state
    if 'db_manager' not in st.session_state:
        st.session_state.db_manager = DatabaseManager()
    if 'translator' not in st.session_state:
        st.session_state.translator = SchemaTranslator()
    if 'current_table_info' not in st.session_state:
        st.session_state.current_table_info = None
    if 'migration_config' not in st.session_state:
        st.session_state.migration_config = {}
    if 'selected_source_type' not in st.session_state:
        st.session_state.selected_source_type = 'oracle'
    if 'selected_target_database' not in st.session_state:
        st.session_state.selected_target_database = 'temp'
    
    # Setup logging
    setup_logging()
    
    # Load configuration
    config = load_config()
    
    # Header
    st.title("🔄 Data Migration Tool")
    st.markdown("Automate database schema migration from Oracle/SQL Server to SQL Server warehouse")
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")
        
        # Source Database connection section
        st.subheader("Source Database Connection")
        source_type = st.selectbox(
            "Source Database Type",
            options=["Oracle", "SQL Server"],
            key="source_database_type"
        )
        
        if source_type == "Oracle":
            render_oracle_connection()
        else:
            render_source_sqlserver_connection()
        
        # Destination Database connections
        st.subheader("Destination Databases")
        dest_tabs = st.tabs(["TEMP Database", "Bronze Database"])
        
        with dest_tabs[0]:
            render_temp_database_connection()
        
        with dest_tabs[1]:
            render_bronze_database_connection()
        
        # Git configuration section
        st.subheader("Git Configuration")
        render_git_configuration()
        
        # Migration settings
        st.subheader("Migration Settings")
        render_migration_settings()
    
    # Main content area
    source_connector = st.session_state.db_manager.get_source_connector()
    if source_connector and source_connector.connection:
        render_main_content()
    else:
        st.info("👈 Please configure and test your source database connection in the sidebar to get started.")


def render_oracle_connection():
    st.write("**Oracle Source Database**")
    
    oracle_username = st.text_input("Username", key="oracle_username")
    oracle_password = st.text_input("Password", type="password", key="oracle_password")
    oracle_dsn = st.text_input("DSN", key="oracle_dsn", help="Format: host:port/service_name or host:port:sid")
    
    if st.button("Test Oracle Connection", key="test_oracle"):
        if oracle_username and oracle_password and oracle_dsn:
            with st.spinner("Testing Oracle connection..."):
                connector = st.session_state.db_manager.create_oracle_source_connector(
                    oracle_username, oracle_password, oracle_dsn
                )
                if connector:
                    st.success("✅ Oracle connection successful!")
                else:
                    st.error("❌ Oracle connection failed")
        else:
            st.warning("Please fill in all Oracle connection fields")


def render_source_sqlserver_connection():
    st.write("**SQL Server Source Database**")
    
    sqlserver_host = st.text_input("Server", key="source_sqlserver_host")
    sqlserver_database = st.text_input("Database", key="source_sqlserver_database")
    
    use_trusted_connection = st.checkbox("Use Trusted Connection", value=True, key="source_use_trusted")
    
    if not use_trusted_connection:
        sqlserver_username = st.text_input("Username", key="source_sqlserver_username")
        sqlserver_password = st.text_input("Password", type="password", key="source_sqlserver_password")
    
    if st.button("Test Source SQL Server Connection", key="test_source_sqlserver"):
        if sqlserver_host and sqlserver_database:
            with st.spinner("Testing SQL Server connection..."):
                if use_trusted_connection:
                    connector = st.session_state.db_manager.create_sqlserver_source_connector(
                        sqlserver_host, sqlserver_database, trusted=True
                    )
                else:
                    connector = st.session_state.db_manager.create_sqlserver_source_connector(
                        sqlserver_host, sqlserver_database, trusted=False,
                        username=st.session_state.get('source_sqlserver_username'),
                        password=st.session_state.get('source_sqlserver_password')
                    )
                if connector:
                    st.success("✅ Source SQL Server connection successful!")
                else:
                    st.error("❌ Source SQL Server connection failed")
        else:
            st.warning("Please fill in server and database fields")


def render_temp_database_connection():
    st.write("**TEMP Database Connection**")
    
    temp_host = st.text_input("Server", key="temp_host")
    temp_database = st.text_input("Database", key="temp_database")
    
    use_trusted_connection = st.checkbox("Use Trusted Connection", value=True, key="temp_use_trusted")
    
    if not use_trusted_connection:
        temp_username = st.text_input("Username", key="temp_username")
        temp_password = st.text_input("Password", type="password", key="temp_password")
    
    if st.button("Test TEMP Database Connection", key="test_temp"):
        if temp_host and temp_database:
            with st.spinner("Testing TEMP database connection..."):
                if use_trusted_connection:
                    connector = st.session_state.db_manager.create_temp_connector(
                        temp_host, temp_database, trusted=True
                    )
                else:
                    connector = st.session_state.db_manager.create_temp_connector(
                        temp_host, temp_database, trusted=False,
                        username=st.session_state.get('temp_username'),
                        password=st.session_state.get('temp_password')
                    )
                if connector:
                    st.success("✅ TEMP database connection successful!")
                else:
                    st.error("❌ TEMP database connection failed")
        else:
            st.warning("Please fill in server and database fields")


def render_bronze_database_connection():
    st.write("**Bronze Database Connection**")
    
    bronze_host = st.text_input("Server", key="bronze_host")
    bronze_database = st.text_input("Database", key="bronze_database")
    
    use_trusted_connection = st.checkbox("Use Trusted Connection", value=True, key="bronze_use_trusted")
    
    if not use_trusted_connection:
        bronze_username = st.text_input("Username", key="bronze_username")
        bronze_password = st.text_input("Password", type="password", key="bronze_password")
    
    if st.button("Test Bronze Database Connection", key="test_bronze"):
        if bronze_host and bronze_database:
            with st.spinner("Testing Bronze database connection..."):
                if use_trusted_connection:
                    connector = st.session_state.db_manager.create_bronze_connector(
                        bronze_host, bronze_database, trusted=True
                    )
                else:
                    connector = st.session_state.db_manager.create_bronze_connector(
                        bronze_host, bronze_database, trusted=False,
                        username=st.session_state.get('bronze_username'),
                        password=st.session_state.get('bronze_password')
                    )
                if connector:
                    st.success("✅ Bronze database connection successful!")
                else:
                    st.error("❌ Bronze database connection failed")
        else:
            st.warning("Please fill in server and database fields")


def render_git_configuration():
    repo_path = st.text_input(
        "Repository Path", 
        value=str(Path.cwd()),
        key="repo_path",
        help="Path to the Git repository containing database projects"
    )
    
    base_branch = st.text_input(
        "Base Branch",
        value="dev",
        key="base_branch",
        help="Branch to create feature branches from"
    )
    
    if st.button("Validate Git Setup", key="validate_git"):
        if repo_path:
            try:
                git_manager = GitManager(repo_path)
                is_valid, message = git_manager.validate_git_setup()
                
                if is_valid:
                    st.success(f"✅ {message}")
                    st.session_state.migration_config['git_manager'] = git_manager
                else:
                    st.error(f"❌ {message}")
            except Exception as e:
                st.error(f"❌ Git validation failed: {e}")


def render_migration_settings():
    source_prefix = st.text_input(
        "Source System Prefix",
        value="src",
        key="source_prefix",
        help="Prefix to add to table names (e.g., 'oracle', 'legacy')"
    )
    
    target_database_name = st.text_input(
        "Target Database Project Name",
        value="DataWarehouse",
        key="target_database_name",
        help="Name of the target database project"
    )
    
    target_database = st.selectbox(
        "Target Database",
        options=["TEMP", "Bronze"],
        key="target_database",
        help="Which database to deploy to"
    )
    
    target_schema = st.selectbox(
        "Target Schema",
        options=["temp_schema", "bronze_schema"],
        index=0 if st.session_state.get('target_database') == 'TEMP' else 1,
        key="target_schema",
        help="Schema to create tables in"
    )
    
    create_view = st.checkbox(
        "Create View",
        value=False,
        key="create_view",
        help="Create a view with original table name"
    )
    
    # Connection status display
    st.write("**Connection Status:**")
    connection_status = st.session_state.db_manager.get_connection_status()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if connection_status['source']:
            st.success("✅ Source")
        else:
            st.error("❌ Source")
    
    with col2:
        if connection_status['temp']:
            st.success("✅ TEMP")
        else:
            st.error("❌ TEMP")
    
    with col3:
        if connection_status['bronze']:
            st.success("✅ Bronze")
        else:
            st.error("❌ Bronze")
    
    # Store settings in session state
    st.session_state.migration_config.update({
        'source_prefix': source_prefix,
        'target_database_name': target_database_name,
        'target_database': target_database,
        'target_schema': target_schema,
        'create_view': create_view
    })


def render_main_content():
    # Table/View selection
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.subheader("📋 Source Selection")
        render_source_selection()
    
    with col2:
        st.subheader("👁️ Schema Preview")
        render_schema_preview()
    
    # DDL Generation and Preview
    st.subheader("🔧 DDL Generation")
    render_ddl_section()
    
    # Migration Execution
    st.subheader("🚀 Migration Execution")
    render_migration_execution()


def render_source_selection():
    # Get available schemas from source connector
    source_connector = st.session_state.db_manager.get_source_connector()
    if not source_connector:
        st.warning("No source database connection available")
        return
        
    schemas = source_connector.get_schemas()
    
    if not schemas:
        st.warning("No schemas found or connection issue")
        return
    
    # Schema selection
    selected_schema = st.selectbox(
        "Select Schema",
        options=schemas,
        key="selected_schema"
    )
    
    if selected_schema:
        # Get tables and views
        tables_and_views = source_connector.get_tables_and_views(selected_schema)
        
        if tables_and_views:
            # Create a DataFrame for better display
            df = pd.DataFrame(tables_and_views)
            
            # Add search functionality
            search_term = st.text_input("🔍 Search tables/views", key="search_term")
            
            if search_term:
                df = df[df['table_name'].str.contains(search_term, case=False, na=False)]
            
            # Display in a nice format
            if not df.empty:
                # Table type filter
                table_types = st.multiselect(
                    "Filter by Type",
                    options=df['table_type'].unique(),
                    default=df['table_type'].unique(),
                    key="table_type_filter"
                )
                
                df_filtered = df[df['table_type'].isin(table_types)]
                
                # Display table
                selected_row = st.dataframe(
                    df_filtered,
                    use_container_width=True,
                    hide_index=True,
                    on_select="rerun",
                    selection_mode="single-row"
                )
                
                # Handle selection
                if selected_row.selection.rows:
                    selected_idx = selected_row.selection.rows[0]
                    selected_table = df_filtered.iloc[selected_idx]
                    
                    # Load table schema
                    with st.spinner("Loading table schema..."):
                        table_info = source_connector.get_table_schema(
                            selected_table['table_name'],
                            selected_table['schema_name']
                        )
                        
                        if table_info:
                            st.session_state.current_table_info = table_info
                            st.success(f"✅ Loaded schema for {selected_table['table_name']}")
                        else:
                            st.error("❌ Failed to load table schema")
            else:
                st.info("No tables/views found matching your criteria")
        else:
            st.warning("No tables or views found in the selected schema")


def render_schema_preview():
    if st.session_state.current_table_info:
        table_info = st.session_state.current_table_info
        
        # Table information
        st.write(f"**Table:** {table_info.table_name}")
        st.write(f"**Schema:** {table_info.schema_name}")
        st.write(f"**Type:** {table_info.table_type}")
        if table_info.row_count:
            st.write(f"**Row Count:** {table_info.row_count:,}")
        
        # Column information
        st.write("**Columns:**")
        
        column_data = []
        for col in table_info.columns:
            column_data.append({
                'Column': col.column_name,
                'Data Type': col.data_type,
                'Nullable': 'Yes' if col.nullable else 'No',
                'Length': col.max_length,
                'Precision': col.precision,
                'Scale': col.scale,
                'PK': '🔑' if col.is_primary_key else ''
            })
        
        st.dataframe(pd.DataFrame(column_data), use_container_width=True)
        
        # Sample data
        if st.button("Show Sample Data", key="show_sample"):
            with st.spinner("Loading sample data..."):
                source_connector = st.session_state.db_manager.get_source_connector()
                sample_data = source_connector.get_sample_data(
                    table_info.table_name,
                    table_info.schema_name,
                    rows=5
                )
                
                if sample_data is not None and not sample_data.empty:
                    st.write("**Sample Data:**")
                    st.dataframe(sample_data, use_container_width=True)
                else:
                    st.warning("No sample data available")
    else:
        st.info("Select a table from the left panel to preview its schema")


def render_ddl_section():
    if not st.session_state.current_table_info:
        st.info("Select a table to generate DDL")
        return
    
    config = st.session_state.migration_config
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("🔧 Generate DDL", key="generate_ddl", type="primary"):
            with st.spinner("Generating DDL..."):
                generate_ddl_preview()
    
    with col2:
        if st.button("📊 Show Storage Impact", key="storage_impact"):
            show_storage_impact()


def generate_ddl_preview():
    table_info = st.session_state.current_table_info
    config = st.session_state.migration_config
    translator = st.session_state.translator
    
    try:
        # Get source connector to determine database type
        source_connector = st.session_state.db_manager.get_source_connector()
        
        # Translate schema if coming from Oracle
        if source_connector.db_type == 'oracle':
            translated_table = translator.translate_oracle_to_sqlserver(table_info)
        else:
            translated_table = table_info
        
        # Add audit columns
        table_with_audit = translator.add_audit_columns(translated_table)
        
        # Apply naming conventions
        target_table_name = translator.apply_naming_conventions(
            table_info.table_name,
            config['source_prefix']
        )
        
        # Generate DDL
        ddl_output = translator.generate_ddl(
            table_with_audit,
            config['target_schema'],
            target_table_name,
            config['create_view']
        )
        
        st.session_state.ddl_output = ddl_output
        
        # Display DDL
        st.subheader("Generated DDL")
        
        # Table DDL
        st.write("**Table DDL:**")
        st.code(ddl_output.table_ddl, language="sql")
        
        # View DDL
        if ddl_output.view_ddl:
            st.write("**View DDL:**")
            st.code(ddl_output.view_ddl, language="sql")
        
        # File structure
        st.write("**Files to be created:**")
        for file_path, _ in ddl_output.files_to_create:
            st.write(f"- {file_path}")
        
        st.success("✅ DDL generated successfully!")
        
    except Exception as e:
        st.error(f"❌ DDL generation failed: {e}")


def show_storage_impact():
    if not st.session_state.current_table_info:
        return
    
    table_info = st.session_state.current_table_info
    translator = st.session_state.translator
    source_connector = st.session_state.db_manager.get_source_connector()
    
    # Translate schema for accurate storage estimation
    if source_connector.db_type == 'oracle':
        translated_table = translator.translate_oracle_to_sqlserver(table_info)
    else:
        translated_table = table_info
    
    storage_impact = translator.estimate_storage_impact(translated_table)
    
    st.subheader("Storage Impact Analysis")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.metric("Estimated Row Size", f"{storage_impact['estimated_row_size_bytes']} bytes")
        st.metric("Variable Length Columns", storage_impact['variable_length_columns'])
    
    with col2:
        if storage_impact['estimated_table_size_mb'] > 0:
            st.metric("Estimated Table Size", f"{storage_impact['estimated_table_size_mb']:.2f} MB")
        
        if storage_impact['has_large_objects']:
            st.warning("⚠️ Contains large object columns")
    
    if storage_impact['performance_notes']:
        st.write("**Performance Notes:**")
        for note in storage_impact['performance_notes']:
            st.write(f"- {note}")


def render_migration_execution():
    if 'ddl_output' not in st.session_state:
        st.info("Generate DDL first to enable migration")
        return
    
    config = st.session_state.migration_config
    
    if 'git_manager' not in config:
        st.warning("⚠️ Git configuration required for migration")
        return
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        branch_name = st.text_input(
            "Branch Name",
            value=f"add_{st.session_state.current_table_info.table_name}",
            key="branch_name"
        )
    
    with col2:
        commit_message = st.text_area(
            "Commit Message",
            value=f"Add migration for {st.session_state.current_table_info.schema_name}.{st.session_state.current_table_info.table_name}",
            key="commit_message"
        )
    
    if st.button("🚀 Execute Migration", key="execute_migration", type="primary"):
        execute_migration(branch_name, commit_message)


def execute_migration(branch_name: str, commit_message: str):
    config = st.session_state.migration_config
    git_manager = config['git_manager']
    ddl_output = st.session_state.ddl_output
    
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        # Step 1: Create feature branch
        status_text.text("Creating feature branch...")
        success, message = git_manager.create_feature_branch(branch_name, config.get('base_branch', 'dev'))
        if not success:
            st.error(f"❌ Failed to create branch: {message}")
            return
        progress_bar.progress(20)
        
        # Step 2: Create directory structure
        status_text.text("Creating directory structure...")
        success, message = git_manager.create_directory_structure(config['target_database_name'])
        if not success:
            st.error(f"❌ Failed to create directories: {message}")
            return
        progress_bar.progress(40)
        
        # Step 3: Write files
        status_text.text("Writing DDL files...")
        success, message, created_files = git_manager.write_files(
            ddl_output.files_to_create,
            config['target_database_name']
        )
        if not success:
            st.error(f"❌ Failed to write files: {message}")
            return
        progress_bar.progress(60)
        
        # Step 4: Update .sqlproj file
        status_text.text("Updating .sqlproj file...")
        sqlproj_entries = st.session_state.translator.generate_sqlproj_entries(ddl_output.files_to_create)
        success, message = git_manager.update_sqlproj(config['target_database_name'], sqlproj_entries)
        if not success:
            st.error(f"❌ Failed to update .sqlproj: {message}")
            return
        progress_bar.progress(80)
        
        # Step 5: Commit changes
        status_text.text("Committing changes...")
        all_files = created_files + [f"{config['target_database_name']}/{config['target_database_name']}.sqlproj"]
        success, message = git_manager.commit_changes(commit_message, all_files)
        if not success:
            st.error(f"❌ Failed to commit changes: {message}")
            return
        progress_bar.progress(100)
        
        status_text.text("Migration completed successfully!")
        
        # Show success message with details
        st.success("🎉 Migration completed successfully!")
        
        current_branch = git_manager.get_current_branch()
        if current_branch:
            st.info(f"📋 Current branch: `{current_branch}`")
        
        st.write("**Created files:**")
        for file_path in created_files:
            st.write(f"- {file_path}")
        
        # Log migration (if logging is configured)
        log_migration_success()
        
    except Exception as e:
        st.error(f"❌ Migration failed: {e}")
        
        # Attempt rollback
        if 'created_files' in locals():
            st.warning("Attempting to rollback changes...")
            success, message = git_manager.rollback_changes(created_files)
            if success:
                st.info(f"✅ Rollback successful: {message}")
            else:
                st.error(f"❌ Rollback failed: {message}")


def log_migration_success():
    # This would integrate with the logging system
    # For now, just log to console
    import logging
    logging.info(f"Migration completed for {st.session_state.current_table_info.table_name}")


if __name__ == "__main__":
    main()