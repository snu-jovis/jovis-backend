# Jovis Backend

## PostgreSQL Setup

The following command uses Docker to run a PostgreSQL server.  
The PostgreSQL settings in `./web/views.py` can be used without modification when executed with the command below.

```bash
docker run --name some-postgres -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres
```

## Logfile Configuration

1. Set the `JOVIS_PG` environment variable to specify the root directory of the PostgreSQL installation:

   ```bash
   export JOVIS_PG=/path/to/postgres
   ```

2. Set the path to the PostgreSQL log file `PG_LOG_FILE` in `./backend/settings.py`.
3. Specify the backup directory `PG_LOG_BACKUP_DIR` for storing old logs.

## Installation

1. Install the required packages:

   ```bash
   pip install -r requirements.txt
   ```

2. Update the database settings:

   - Open `./web/views.py` and locate the `psycopg2.connect()` part in the `QueryView.post()` function.
   - Modify the parameters to match your database settings.

3. (Optional) If prompted to perform a migration, run:

   ```bash
   python manage.py migrate
   ```

4. Start the server:

   ```bash
   python manage.py runserver
   ```
