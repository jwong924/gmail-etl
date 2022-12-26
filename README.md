# gmail-etl
Google API OAuth2 Account Required


# PostgreSQL Details
```bash
export POSTGRESQL_HOST=127.0.0.1
export POSTGRESQL_PORT=5432
export POSTGRESQL_USER='gmail_user'
export POSTGRESQL_PASSWORD='Gmail$123'
```


# Airflow Commands
```bash
airflow db init
airflow webserver --port 8080
airflow scheduler
```