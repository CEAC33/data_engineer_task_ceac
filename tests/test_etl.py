import findspark
findspark.init()

import unittest
from etl.etl import NetflixTitlesETL

class SparkETLTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.etl = NetflixTitlesETL()

    @classmethod
    def tearDownClass(self):
        self.etl.spark.stop()

    def test_csv_df(self):
        csv_df = self.etl.csv_df

        self.assertEqual(csv_df.count(), 7787)
        self.assertEqual(csv_df.columns, ['show_id', 'type', 'title', 'director', 'cast', 'country', 'date_added', 'release_year', 'rating', 'duration', 'listed_in', 'description'])

    def test_get_types(self):
        types = self.etl.get_types(write_to_db=False)

        self.assertEqual(types.count(), 2)
        self.assertEqual(types.columns, ['type', 'type_id'])

    def test_get_people_roles(self):
        people_roles = self.etl.get_people_roles(write_to_db=False)

        self.assertEqual(people_roles.count(), 2)
        self.assertEqual(people_roles.columns, ['role', 'role_id'])

    def test_get_directors(self):
        directors = self.etl.get_directors(write_to_db=False)

        self.assertEqual(directors.count(), 4049)
        self.assertEqual(directors.columns, ['name', 'people_id', 'role_id', 'gender'])

    def test_get_cast_members(self):
        self.etl.get_directors(write_to_db=False)
        cast_members = self.etl.get_cast_members(write_to_db=False)

        self.assertEqual(cast_members.count(), 32881)
        self.assertEqual(cast_members.columns, ['name', 'people_id', 'role_id', 'gender'])

    def test_get_countries(self):
        countries = self.etl.get_countries(write_to_db=False)

        self.assertEqual(countries.count(), 121)
        self.assertEqual(countries.columns, ['country', 'country_id'])

    def test_get_ratings(self):
        ratings = self.etl.get_ratings(write_to_db=False)

        self.assertEqual(ratings.count(), 14)
        self.assertEqual(ratings.columns, ['rating', 'rating_id'])

    def test_get_categories(self):
        categories = self.etl.get_categories(write_to_db=False)

        self.assertEqual(categories.count(), 42)
        self.assertEqual(categories.columns, ['category', 'category_id'])

    def test_get_shows(self):
        shows = self.etl.get_shows(write_to_db=False)
        csv_df = self.etl.csv_df

        self.assertEqual(shows.count(), csv_df.count())
        self.assertEqual(shows.columns, ['show_id', 'title', 'release_year', 'description', 'type_id', 'director_id', 'country_id', 'rating_id', 'date_added', 'duration_amount', 'duration_unit'])

    def test_get_show_categories(self):
        show_categories = self.etl.get_show_categories(write_to_db=False)

        self.assertEqual(show_categories.count(), 17071)
        self.assertEqual(show_categories.columns, ['show_id', 'category_id'])

    def test_get_show_cast_members(self):
        show_cast_members = self.etl.get_show_cast_members(write_to_db=False)

        self.assertEqual(show_cast_members.count(), 55955)
        self.assertEqual(show_cast_members.columns, ['show_id', 'people_id'])