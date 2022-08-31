pyspark_image_name := 'pyspark-example:dev'
db_image_name := 'postgres:12-alpine'
postgres_user := 'root'
postgres_password := 'secret'
postgres_host := 'docker_postgres_with_data_postgres_1'
postgres_port := '5432'
postgres_db_name := 'netflix_titles'
path_in_your_local := '/Users/macbook/Documents/LinkFire/data_engineer_task/'

build:
    docker build -t {{db_image_name}} .
    docker build -t {{pyspark_image_name}} .

run_db:
    docker pull postgres:12-alpine
    docker run --name postgres_db -e POSTGRES_USER=root -e POSTGRES_PASSWORD=secret -p 5432:5432 -d postgres:12-alpine

run_pyspark:
    docker run --detach -it --name pyspark_notebook_1 --network host \
    {{pyspark_image_name}} /opt/spark/bin/pyspark --packages com.amazonaws:aws-java-sdk-bundle:1.11.375,org.apache.hadoop:hadoop-aws:3.2.0 

start:
    docker start postgres_db
    docker start pyspark_notebook_1

shell_db:    
    docker exec -it postgres_db psql -U root

copy_csv:
    docker cp {{path_in_your_local}}netflix_titles.csv pyspark_notebook_1:/opt/application/netflix_titles.csv

run_etl:
    docker exec -it pyspark_notebook_1 /opt/spark/bin/spark-submit "Step_2_&_3_ETL_and_gender_detection.py" --packages com.amazonaws:aws-java-sdk-bundle:1.11.375,org.apache.hadoop:hadoop-aws:3.2.0 

shell:
    docker exec -it pyspark_notebook_1 /opt/spark/bin/pyspark --packages com.amazonaws:aws-java-sdk-bundle:1.11.375,org.apache.hadoop:hadoop-aws:3.2.0 