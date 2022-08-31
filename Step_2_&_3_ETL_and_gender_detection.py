
from pyspark.sql import SparkSession        
import os
from pyspark.sql.functions import col, lit, split, row_number, udf, ltrim, explode, monotonically_increasing_id
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from dateutil.parser import parse
import gender_guesser.detector as gender

# TODO: Improve security putting this in a env file or something
os.environ["PG_USER"] = "root"
os.environ["PG_PASSWORD"] = "secret"
os.environ["PG_HOST"] = "localhost"
os.environ["PG_PORT"] = "5432"
os.environ["PG_DB_NAME"] = "netflix_titles"

spark = (SparkSession
.builder
.appName("DataEngineerTask")
.getOrCreate())

csv_file = "netflix_titles.csv"

csv_df = (spark.read.format("csv")
.option("inferSchema", "true")
.option("header", "true")
.load(csv_file))
csv_df.createOrReplaceTempView("netflix_titles")

csv_df = csv_df.withColumn("release_year", csv_df["release_year"].cast("int"))
csv_df.printSchema()

# SQL metadata
properties = {"user": os.environ["PG_USER"],"password": os.environ["PG_PASSWORD"],"driver": "org.postgresql.Driver"}
url = f"jdbc:postgresql://{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB_NAME']}"

# write to db

def add_serial_column(df, serial_column, serial_offset=0):
    df = df.withColumn("idx", monotonically_increasing_id())
    w = Window().orderBy("idx")
    return df.withColumn(serial_column, (serial_offset + row_number().over(w))).drop("idx")

def get_uniques_in_df(column, serial_column, serial_offset=0):
    df = spark.createDataFrame(csv_df.select(column).distinct().where(col(column).isNotNull()).collect())
    return add_serial_column(df, serial_column, serial_offset)

def get_uniques_in_df_split(column, serial_column, serial_offset=0):
    unique_values = set()
    for row in csv_df.collect():
        if row[column]: 
            values_list = row[column].split(", ")
            unique_values.update(values_list)
    df = spark.createDataFrame(unique_values, StringType()).toDF(*[column])
    return add_serial_column(df, serial_column, serial_offset)


detector = gender.Detector()
get_gender = udf (lambda name: detector.get_gender(name.split()[0]).replace("mostly_", "")[0], StringType())


types = get_uniques_in_df("type", "type_id")
types.write.jdbc(url=url, table="show_types", mode="append", properties=properties)

people_roles = add_serial_column(spark.createDataFrame(["director","cast"], StringType()).toDF(*["role"]),"role_id")
people_roles.write.jdbc(url=url, table="people_roles", mode="append", properties=properties)

directors = get_uniques_in_df("director", "people_id").withColumnRenamed("director","name").withColumn("role_id", lit(1))
directors = directors.withColumn("gender", get_gender(directors["name"]))
directors.write.jdbc(url=url, table="people", mode="append", properties=properties)

cast_members = get_uniques_in_df_split("cast", "people_id", directors.count()).withColumnRenamed("cast","name").withColumn("role_id", lit(2))
cast_members = cast_members.withColumn("gender", get_gender(cast_members["name"]))
cast_members = cast_members.withColumn("gender", get_gender(cast_members["name"]))
cast_members.write.jdbc(url=url, table="people", mode="append", properties=properties)

countries = get_uniques_in_df_split("country", "country_id")
countries.write.jdbc(url=url, table="countries", mode="append", properties=properties)

ratings = get_uniques_in_df("rating", "rating_id")
ratings.write.jdbc(url=url, table="ratings", mode="append", properties=properties)

categories = get_uniques_in_df_split("listed_in", "category_id").withColumnRenamed("listed_in","category")
categories.write.jdbc(url=url, table="categories", mode="append", properties=properties)

shows = csv_df[["show_id", "type", "title", "director", "country", "date_added", "release_year", "rating", "duration", "description"]]
shows = shows.join(types, shows.type ==  types.type,"left").drop("type")
shows = shows.join(directors[["name", "people_id"]].withColumnRenamed("people_id","director_id"), 
    shows.director ==  directors.name,"left").drop("director","name")
shows = shows.join(countries, shows.country ==  countries.country,"left").drop("country")
shows = shows.join(ratings, shows.rating ==  ratings.rating,"left").drop("rating")
parse_date =  udf (lambda x: parse(x).strftime("%Y-%m-%d") if x else None, StringType())
shows = shows.withColumn("date_added_parsed", parse_date(shows["date_added"])).drop("date_added").withColumnRenamed("date_added_parsed","date_added")
shows = shows.withColumn("date_added", shows["date_added"].cast("date"))
shows = shows\
.withColumn("duration_amount", split(ltrim(col("duration")), " ").getItem(0))\
.withColumn("duration_unit", split(ltrim(col("duration")), " ").getItem(1))\
.drop("duration")
shows = shows.withColumn("duration_amount", shows["duration_amount"].cast("int"))
shows.write.jdbc(url=url, table="shows", mode="append", properties=properties)

show_categories = csv_df[["show_id", "listed_in"]].withColumnRenamed("listed_in","category")
show_categories = show_categories.withColumn("category",explode(split("category",", ")))
show_categories = show_categories.join(categories, show_categories.category == categories.category,"left").drop("category")
show_categories.write.jdbc(url=url, table="show_categories", mode="append", properties=properties)

show_cast_members = csv_df[["show_id", "cast"]]
show_cast_members = show_cast_members.withColumn("cast_member",explode(split("cast",", "))).drop("cast")
show_cast_members = show_cast_members.join(cast_members[["name", "people_id"]], show_cast_members.cast_member == cast_members.name,"left").drop("name", "cast_member")
show_cast_members.write.jdbc(url=url, table="show_cast_members", mode="append", properties=properties)