import findspark
findspark.init()


from pyspark.sql import SparkSession        
import os
from pyspark.sql.functions import col, lit, split, row_number, udf, ltrim, explode, monotonically_increasing_id
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from dateutil.parser import parse
import gender_guesser.detector as gender

detector = gender.Detector()
get_gender = udf (lambda name: detector.get_gender(name.split()[0]).replace("mostly_", "")[0], StringType())

# TODO: Improve security putting this in a env file or something
# os.environ["PG_USER"] = "root"
# os.environ["PG_PASSWORD"] = "secret"
# os.environ["PG_HOST"] = "localhost"
# os.environ["PG_PORT"] = "5432"
# os.environ["PG_DB_NAME"] = "netflix_titles"

CSV_FILE = "netflix_titles.csv"

class NetflixTitlesETL:

    def __init__(self):
        self.spark = (SparkSession
        .builder
        .appName("DataEngineerTask")
        .getOrCreate())

        self.csv_df = (self.spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(CSV_FILE))
        self.csv_df.createOrReplaceTempView(CSV_FILE.replace(".csv", ""))

        self.csv_df = self.csv_df.withColumn("release_year", self.csv_df["release_year"].cast("int"))
        self.csv_df.printSchema()

        # SQL metadata
        self.properties = {"user": os.environ["POSTGRES_USER"],"password": os.environ["POSTGRES_PASSWORD"],"driver": "org.postgresql.Driver"}
        self.url = f"jdbc:postgresql://{os.environ['POSTGRES_HOST']}:{os.environ['POSTGRES_PORT']}/{os.environ['POSTGRES_DB_NAME']}"

    def add_serial_column(self, df, serial_column, serial_offset=0):
        df = df.withColumn("idx", monotonically_increasing_id())
        w = Window().orderBy("idx")
        return df.withColumn(serial_column, (serial_offset + row_number().over(w))).drop("idx")

    def get_uniques_in_df(self, column, serial_column, serial_offset=0):
        df = self.spark.createDataFrame(self.csv_df.select(column).distinct().where(col(column).isNotNull()).collect())
        return self.add_serial_column(df, serial_column, serial_offset)

    def get_uniques_in_df_split(self, column, serial_column, serial_offset=0):
        unique_values = set()
        for row in self.csv_df.collect():
            if row[column]: 
                values_list = row[column].split(", ")
                unique_values.update(values_list)
        df = self.spark.createDataFrame(unique_values, StringType()).toDF(*[column])
        return self.add_serial_column(df, serial_column, serial_offset)

    def write_to_db(self, df, table):
        df.write.jdbc(url=self.url, table=table, mode="append", properties=self.properties)

    def get_types(self, write_to_db=False):
        types = self.get_uniques_in_df("type", "type_id")
        if write_to_db: self.write_to_db(types, "show_types")
        self.types = types
        return types

    def get_people_roles(self, write_to_db=False):
        people_roles = self.add_serial_column(self.spark.createDataFrame(["director","cast"], StringType()).toDF(*["role"]),"role_id")
        if write_to_db: self.write_to_db(people_roles, "people_roles")
        self.people_roles = people_roles
        return people_roles

    def get_directors(self, write_to_db=False):
        directors = self.get_uniques_in_df("director", "people_id").withColumnRenamed("director","name").withColumn("role_id", lit(1))
        directors = directors.withColumn("gender", get_gender(directors["name"]))
        if write_to_db: self.write_to_db(directors, "people")
        self.directors = directors
        return directors

    def get_cast_members(self, write_to_db=False):
        if not hasattr(self, 'directors'): self.get_directors()
        cast_members = self.get_uniques_in_df_split("cast", "people_id", self.directors.count()).withColumnRenamed("cast","name").withColumn("role_id", lit(2))
        cast_members = cast_members.withColumn("gender", get_gender(cast_members["name"]))
        if write_to_db: self.write_to_db(cast_members, "people")
        self.cast_members = cast_members
        return cast_members

    def get_countries(self, write_to_db=False):
        countries = self.get_uniques_in_df_split("country", "country_id")
        if write_to_db: self.write_to_db(countries, "countries")
        self.countries = countries
        return countries

    def get_ratings(self, write_to_db=False):
        ratings = self.get_uniques_in_df("rating", "rating_id")
        if write_to_db: self.write_to_db(ratings, "ratings")
        self.ratings = ratings
        return ratings

    def get_categories(self, write_to_db=False):
        categories = self.get_uniques_in_df_split("listed_in", "category_id").withColumnRenamed("listed_in","category")
        if write_to_db: self.write_to_db(categories, "categories")
        self.categories = categories
        return categories

    def get_shows(self, write_to_db=False):
        if not hasattr(self, 'types'): self.get_types()
        if not hasattr(self, 'directors'): self.get_directors()
        if not hasattr(self, 'countries'): self.get_countries()
        if not hasattr(self, 'ratings'): self.get_ratings()
        shows = self.csv_df[["show_id", "type", "title", "director", "country", "date_added", "release_year", "rating", "duration", "description"]]
        shows = shows.join(self.types, shows.type ==  self.types.type,"left").drop("type")
        shows = shows.join(self.directors[["name", "people_id"]].withColumnRenamed("people_id","director_id"), 
            shows.director ==  self.directors.name,"left").drop("director","name")
        shows = shows.join(self.countries, shows.country == self.countries.country,"left").drop("country")
        shows = shows.join(self.ratings, shows.rating == self.ratings.rating,"left").drop("rating")
        parse_date =  udf (lambda x: parse(x).strftime("%Y-%m-%d") if x else None, StringType())
        shows = shows.withColumn("date_added_parsed", parse_date(shows["date_added"])).drop("date_added").withColumnRenamed("date_added_parsed","date_added")
        shows = shows.withColumn("date_added", shows["date_added"].cast("date"))
        shows = shows\
        .withColumn("duration_amount", split(ltrim(col("duration")), " ").getItem(0))\
        .withColumn("duration_unit", split(ltrim(col("duration")), " ").getItem(1))\
        .drop("duration")
        shows = shows.withColumn("duration_amount", shows["duration_amount"].cast("int"))
        if write_to_db: self.write_to_db(shows, "shows")
        self.shows = shows
        return shows

    def get_show_categories(self, write_to_db=False):
        show_categories = self.csv_df[["show_id", "listed_in"]].withColumnRenamed("listed_in","category")
        show_categories = show_categories.withColumn("category",explode(split("category",", ")))
        show_categories = show_categories.join(self.categories, show_categories.category == self.categories.category,"left").drop("category")
        if write_to_db: self.write_to_db(show_categories, "show_categories")
        self.show_categories = show_categories
        return show_categories

    def get_show_cast_members(self, write_to_db=False):
        if not hasattr(self, 'cast_members'): self.get_cast_members()
        show_cast_members = self.csv_df[["show_id", "cast"]]
        show_cast_members = show_cast_members.withColumn("cast_member",explode(split("cast",", "))).drop("cast")
        show_cast_members = show_cast_members.join(self.cast_members[["name", "people_id"]], show_cast_members.cast_member == self.cast_members.name,"left").drop("name", "cast_member")
        if write_to_db: self.write_to_db(show_cast_members, "show_cast_members")
        self.show_cast_members = show_cast_members
        return show_cast_members