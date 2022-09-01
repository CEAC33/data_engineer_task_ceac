set dotenv-load := true
PATH_IN_YOUR_LOCAL := '/Users/macbook/Documents/LinkFire/data_engineer_task/'

build:
    docker build -t $DB_IMAGE_NAME .
    docker build -t $PYSPARK_IMAGE_NAME .

run_db:
    docker pull postgres:12-alpine
    docker run --name postgres_db -e POSTGRES_USER=$POSTGRES_USER -e POSTGRES_PASSWORD=$POSTGRES_PASSWORD -p $POSTGRES_PORT:$POSTGRES_PORT -d postgres:12-alpine

run_pyspark:
    docker run --env-file .env --detach -it --name pyspark_notebook_1 --network host \
    $PYSPARK_IMAGE_NAME /opt/spark/bin/pyspark 

start:
    docker start postgres_db
    docker start pyspark_notebook_1

shell_db:    
    docker exec -it postgres_db psql -U root

copy_csv:
    docker cp {{PATH_IN_YOUR_LOCAL}}netflix_titles.csv pyspark_notebook_1:/opt/application/netflix_titles.csv

run_etl:
    docker exec -it pyspark_notebook_1 python run_etl.py

pyspark_shell:
    docker exec -it pyspark_notebook_1 /opt/spark/bin/pyspark

tests:
    docker exec -it pyspark_notebook_1 python -m unittest -v