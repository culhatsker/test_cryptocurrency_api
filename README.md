# A Simple API for cryptocurrency rates made with aiohttp and asyncpg

To run it, a postgres instance is required.

Create a database in this instance and run (add PG_DATABASE variable otherwise
"bc_api_data" database name is expected)

```
PG_USERNAME=<username> PG_PASSWORD=<password> PG_HOST=<instance_ip_or_host> python3 start_app.py
```

Schedule running `start_background_worker.py` to update data from time to time.
It uses the same environment variables to connect to database:
```
PG_USERNAME=<username> PG_PASSWORD=<password> PG_HOST=<instance_ip_or_host> python3 start_background_worker.py
```
