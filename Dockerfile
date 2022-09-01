from gcr.io/datamechanics/spark:platform-3.1-dm14

ENV PYSPARK_MAJOR_PYTHON_VERSION=3
WORKDIR /opt/application/

RUN wget  https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
RUN mv postgresql-42.2.5.jar /opt/spark/jars

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY netflix_titles.csv .
COPY main.py .
COPY run_etl.py .
ADD etl /opt/application/etl
ADD sql /opt/application/sql
ADD tests /opt/application/tests

RUN PYTHONPATH=. python main.py