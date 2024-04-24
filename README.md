# Jovis Backend

## Installation

1. Use the following command to install the requirements.
```bash
pip install -r requirements.txt
```

2. Please change the database settings.
Open the `./web/views.py` file and check the `psycopg2.connect()` part in the `QueryView.post()` function.
Modify the parameters appropriately according to your database settings.

3. (Optional) If getting a warning to perform a migration, so use the following command to carry out the Django migration.
```bash
python manage.py migrate
```

4. Use the following command to start the server.
```bash
python manage.py runserver
```


## PostgreSQL Setup

The following command uses Docker to run a PostgreSQL server.
The PostgreSQL settings in `./web/views.py` can be used without modification when executed with the command below.
```bash
docker run --name some-postgres -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres
```
