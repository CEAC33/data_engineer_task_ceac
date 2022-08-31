from gcr.io/datamechanics/spark:platform-3.1-dm14

ENV PYSPARK_MAJOR_PYTHON_VERSION=3
WORKDIR /opt/application/

RUN wget  https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
RUN mv postgresql-42.2.5.jar /opt/spark/jars

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY main.py .
COPY netflix_titles.csv .
COPY "Step_1_db_schema_script.sql" .
COPY "Step_2_&_3_ETL_and_gender_detection.py" .
COPY "Step_4_validation_scripts.sql" .
COPY "Step_5_data_analysis_scripts.sql" .
COPY "Step_6_sql_queries_with_unit_tests.py" .