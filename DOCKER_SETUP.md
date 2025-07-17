# Docker Setup Guide

This guide will help you build and run the Data Migration Tool using Docker.

## Prerequisites

- Docker installed on your system
- Docker Compose (included with Docker Desktop)

## Quick Start

### 1. Clone and Navigate to the Project
```bash
git clone <repository-url>
cd table_migration
```

### 2. Configure Environment Variables
Copy the example environment file and edit it with your configuration:
```bash
cp .env.example .env
# Edit .env with your database connection details
```

### 3. Build and Run with Docker Compose
```bash
docker-compose up -d
```

### 4. Access the Application
Open your browser and go to:
```
http://localhost:8501
```

### 5. Stop the Application
```bash
docker-compose down
```

## Alternative: Direct Docker Commands

### Build the Image
```bash
docker build -t data-migration-tool .
```

### Run the Container
```bash
docker run -d -p 8501:8501 --name data-migration-tool data-migration-tool
```

### Stop and Remove
```bash
docker stop data-migration-tool
docker rm data-migration-tool
```

## Repository Integration Options

### Option 1: Auto-Clone Repository (Recommended for Production)

The container can automatically clone your Git repository on startup:

1. **Enable auto-clone in your .env file:**
```env
GIT_AUTO_CLONE=true
GIT_REPO_URL=https://github.com/aaro21/table_migration.git
GIT_REPO_BRANCH=main
```

2. **Run the container:**
```bash
docker-compose up -d
```

The repository will be automatically cloned to `/app/repos/table_migration` inside the container.

### Option 2: Volume Mount Local Repository (Best for Development)

Mount your local repository for development:

```bash
docker run -d -p 8501:8501 \
  -v $(pwd):/app/repos/table_migration \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/logs:/app/logs \
  --name data-migration-tool \
  data-migration-tool
```

### Option 3: Custom Repository Configuration

Override the default repository settings:

```bash
docker run -d -p 8501:8501 \
  -e GIT_AUTO_CLONE=true \
  -e GIT_REPO_URL=https://github.com/your-org/your-repo.git \
  -e GIT_REPO_BRANCH=develop \
  --name data-migration-tool \
  data-migration-tool
```

## Health Check

The container includes a health check that verifies the Streamlit application is running:

```bash
docker-compose ps
```

You should see status as "healthy" when the application is ready.

## Troubleshooting

### Container Won't Start
1. Check Docker logs: `docker-compose logs`
2. Verify .env file exists and has correct format
3. Ensure ports 8501 is not already in use

### Database Connection Issues
1. Verify your database connection strings in .env
2. Ensure database servers are accessible from the container
3. Check if Oracle client libraries are properly installed (for Oracle connections)

### Git Integration Issues
1. Mount your Git repository directory to `/app/repos`
2. Ensure proper Git configuration in the mounted directory

## Container Information

- **Base Image**: python:3.11-slim
- **Exposed Port**: 8501
- **Working Directory**: /app
- **Environment**: Streamlit application with database drivers
- **Health Check**: HTTP check on port 8501

## Security Notes

- The container runs as root by default
- For production use, consider creating a non-root user
- Database credentials are passed via environment variables
- Ensure .env file is not committed to version control