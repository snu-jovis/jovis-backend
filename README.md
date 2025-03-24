# Jovis Backend

Backend service for Jovis, providing APIs and processing logic to visualize and explore the PostgreSQL query optimization process.

## Overview

Jovis Backend connects to a PostgreSQL server, processes query execution data, and delivers it to the frontend for interactive visualization.

## Logfile Configuration

1. **Set the PostgreSQL root directory**  
   Export the environment variable `JOVIS_PG` to point to your PostgreSQL installation root:

   ```bash
   export JOVIS_PG=/path/to/postgres
   ```

2. **Configure log file path**  
   In `./backend/settings.py`, set the following variables:
   - `PG_LOG_FILE`: Path to the active PostgreSQL log file
     - Default: `${JOVIS_PG}/logfile`
   - `PG_LOG_BACKUP_DIR`: Directory for storing old log backups default= JOVIS_PG/backup
     - Default: `${JOVIS_PG}/backup`

## Installation

1. **Install Python dependencies**

   ```bash
   pip install -r requirements.txt
   ```

2. **Update database connection settings**

   - Open `./web/views.py`
   - Locate the `psycopg2.connect()` function inside `QueryView.post()`
   - Modify the connection parameters to match your PostgreSQL instance (host, port, user, password, database)

3. **(Optional) Perform Django migrations**  
   If you are prompted for migrations, run:

   ```bash
   python manage.py migrate
   ```

## Running the Backend Server

Start the Django development server:

```bash
python manage.py runserver
```

By default, the server runs at http://localhost:8000 (or the configured port)
