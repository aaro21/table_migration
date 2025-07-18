FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    unixodbc-dev \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver for SQL Server
RUN apt-get update \
    && apt-get install -y gnupg2 \
    && curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor > /etc/apt/trusted.gpg.d/microsoft.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/etc/apt/trusted.gpg.d/microsoft.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p /app/logs /app/repos /app/config

# Copy and set permissions for startup script
COPY startup.sh /app/startup.sh
RUN chmod +x /app/startup.sh

# Expose Streamlit port
EXPOSE 8501

# Set environment variables
ENV PYTHONPATH=/app/src
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_ADDRESS=0.0.0.0

# Git repository configuration (can be overridden)
ENV GIT_REPO_URL="https://github.com/aaro21/table_migration.git"
ENV GIT_REPO_BRANCH="main"
ENV GIT_AUTO_CLONE="false"

# Run the startup script
CMD ["/app/startup.sh"]