from etl.etl import NetflixTitlesETL

etl = NetflixTitlesETL()

etl.get_types(write_to_db=True)
etl.get_people_roles(write_to_db=True)
etl.get_directors(write_to_db=True)
etl.get_cast_members(write_to_db=True)
etl.get_countries(write_to_db=True)
etl.get_ratings(write_to_db=True)
etl.get_categories(write_to_db=True)
etl.get_shows(write_to_db=True)
etl.get_show_categories(write_to_db=True)
etl.get_show_cast_members(write_to_db=True)