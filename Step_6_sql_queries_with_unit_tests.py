import unittest

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

people_roles = add_serial_column(spark.createDataFrame(["director","cast"], StringType()).toDF(*["role"]),"role_id")

directors = get_uniques_in_df("director", "people_id").withColumnRenamed("director","name").withColumn("role_id", lit(1))
directors = directors.withColumn("gender", get_gender(directors["name"]))

cast_members = get_uniques_in_df_split("cast", "people_id", directors.count()).withColumnRenamed("cast","name").withColumn("role_id", lit(2))
cast_members = cast_members.withColumn("gender", get_gender(cast_members["name"]))
cast_members = cast_members.withColumn("gender", get_gender(cast_members["name"]))

countries = get_uniques_in_df_split("country", "country_id")

ratings = get_uniques_in_df("rating", "rating_id")

categories = get_uniques_in_df_split("listed_in", "category_id").withColumnRenamed("listed_in","category")

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

show_categories = csv_df[["show_id", "listed_in"]].withColumnRenamed("listed_in","category")
show_categories = show_categories.withColumn("category",explode(split("category",", ")))
show_categories = show_categories.join(categories, show_categories.category == categories.category,"left").drop("category")

show_cast_members = csv_df[["show_id", "cast"]]
show_cast_members = show_cast_members.withColumn("cast_member",explode(split("cast",", "))).drop("cast")
show_cast_members = show_cast_members.join(cast_members[["name", "people_id"]], show_cast_members.cast_member == cast_members.name,"left").drop("name", "cast_member")

class TestStringMethods(unittest.TestCase):

    def test_csv_df(self):
        self.assertEqual(csv_df.count(), 7787)
        self.assertEqual(types.csv_df, ['show_id', 'type', 'title', 'director', 'cast', 'country', 'date_added', 'release_year', 'rating', 'duration', 'listed_in', 'description'])

    def test_types(self):
        self.assertEqual(types.count(), 2)
        self.assertEqual(types.columns, ['type', 'type_id'])

    def test_people_roles(self):
        self.assertEqual(people_roles.count(), 2)
        self.assertEqual(people_roles.columns, ['role', 'role_id'])

    def test_directors(self):
        self.assertEqual(directors.count(), 4049)
        self.assertEqual(directors.columns, ['name', 'people_id', 'role_id', 'gender'])

    def test_cast_members(self):
        self.assertEqual(cast_members.count(), 32881)
        self.assertEqual(cast_members.columns, ['name', 'people_id', 'role_id', 'gender'])

    def test_countries(self):
        self.assertEqual(countries.count(), 121)
        self.assertEqual(countries.columns, ['country', 'country_id'])

    def test_ratings(self):
        self.assertEqual(ratings.count(), 14)
        self.assertEqual(ratings.columns, ['rating', 'rating_id'])

    def test_categories(self):
        self.assertEqual(categories.count(), 42)
        self.assertEqual(categories.columns, ['category', 'category_id'])

    def test_shows(self):
        self.assertEqual(shows.count(), csv_df.count())
        self.assertEqual(shows.columns, ['show_id', 'title', 'release_year', 'description', 'type_id', 'director_id', 'country_id', 'rating_id', 'date_added', 'duration_amount', 'duration_unit'])

    def test_show_categories(self):
        self.assertEqual(show_categories.count(), 17071)
        self.assertEqual(show_categories.columns, ['show_id', 'category_id'])

    def test_show_cast_members(self):
        self.assertEqual(show_cast_members.count(), 55955)
        self.assertEqual(show_cast_members.columns, ['show_id', 'people_id'])

if __name__ == '__main__':
    unittest.main()