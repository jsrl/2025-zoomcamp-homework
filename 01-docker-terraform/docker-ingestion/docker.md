# Docker

## Run the script with Docker Compose
```bash
docker-compose up -d
docker-compose down
```
## Running Containers
```bash
docker run -it \
  --network=2_docker_sql_my_network \
  taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz
```

## Verify Docker network
List all networks:
```bash
docker network ls
```
Inspect a specific network:
```bash
docker network inspect 2_docker_sql_default
```

### Interactive Bash Shell
```bash
docker run -it --entrypoint=bash python:3.12.8
```
Run a bash shell in an interactive session using the `python:3.12.8` image.

## Jupyter Notebook
```bash
jupyter nbconvert --to=script homework1.ipynb
```
Convert the Jupyter Notebook (`homework1.ipynb`) to a Python script.


## Using pgcli: Postgres Client for Python

```bash
pgcli -h localhost -U root -d ny_taxi
\dt
```