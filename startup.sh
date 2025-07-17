#!/bin/bash

# Startup script for Data Migration Tool
echo "🚀 Starting Data Migration Tool..."

# Repository configuration
REPO_URL=${GIT_REPO_URL:-"https://github.com/aaro21/table_migration.git"}
REPO_BRANCH=${GIT_REPO_BRANCH:-"main"}
REPO_DIR="/app/repos/table_migration"
AUTO_CLONE=${GIT_AUTO_CLONE:-"false"}

# Auto-clone repository if enabled
if [ "$AUTO_CLONE" = "true" ]; then
    echo "📦 Auto-clone enabled..."
    
    if [ ! -d "$REPO_DIR" ]; then
        echo "📥 Cloning repository: $REPO_URL"
        mkdir -p /app/repos
        git clone "$REPO_URL" "$REPO_DIR"
        
        if [ $? -eq 0 ]; then
            echo "✅ Repository cloned successfully"
        else
            echo "❌ Failed to clone repository"
        fi
    else
        echo "📁 Repository exists, pulling latest changes..."
        cd "$REPO_DIR"
        git pull origin "$REPO_BRANCH"
        
        if [ $? -eq 0 ]; then
            echo "✅ Repository updated successfully"
        else
            echo "❌ Failed to update repository"
        fi
    fi
    
    # Set proper permissions
    chown -R $(id -u):$(id -g) /app/repos 2>/dev/null || true
fi

# Display environment info
echo "🔧 Environment Information:"
echo "   - Repository URL: $REPO_URL"
echo "   - Repository Branch: $REPO_BRANCH"
echo "   - Auto-clone: $AUTO_CLONE"
echo "   - Repository Directory: $REPO_DIR"

# Check if repository directory exists
if [ -d "$REPO_DIR" ]; then
    echo "✅ Repository available at: $REPO_DIR"
else
    echo "⚠️  Repository not found. To auto-clone, set GIT_AUTO_CLONE=true"
fi

echo "🌐 Starting Streamlit application on port 8501..."

# Start the Streamlit application
exec streamlit run app.py --server.port=8501 --server.address=0.0.0.0