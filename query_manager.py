import logging
import pymysql #
import json
import os
from db_connection import DBConnection
from sql_queries import FilmQueries

class QueryHandler(DBConnection):
    """ Initializes the request handler. """
    def __init__(self, dbconfig, log_file='app.log', query_log_file='query_log.log', count_file='query_counts.json'):
        super().__init__(dbconfig, log_file)
        self.query_log_file = query_log_file
        self.count_file = count_file
        self.query_counts = self.load_query_counts()
        self.conn = pymysql.connect(**dbconfig) #
        self.cursor = self.conn.cursor(pymysql.cursors.DictCursor) #

    def log_query(self, query, params):
        """ Logs the executed query, updates the counters, and saves them. """
        param_str = ', '.join(map(str.strip, map(str, params)))

        self.query_counts[param_str] = self.query_counts.get(param_str, 0) + 1

        with open(self.query_log_file, 'a') as f:
            f.write(f"Query: {query}\nParams: ({param_str})\n")
            f.write(f"Total Execution for this Query: {self.query_counts.get(param_str)}\n\n")
        self.save_query_counts()

        print(f"Params: ({param_str})")
        print(f"Total Execution for this Query: {self.query_counts.get(param_str)}")

    def load_query_counts(self):
        """ Loads data about popular queries from a JSON file. """
        if os.path.exists(self.count_file):
            with open(self.count_file, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    return {}
        return {}

    def save_query_counts(self):
        """ Saves the current popular queries to a JSON file. """
        with open(self.count_file, 'w') as f:
            json.dump(self.query_counts, f, indent=4)

    def get_popular_queries(self, top_n=3):
        """ Displays the top_n most popular queries. """
        self.query_counts = self.load_query_counts()
        sorted_queries = sorted(self.query_counts.items(), key=lambda x: x[1], reverse=True)

        if not sorted_queries:
            print("There are no popular queries.")
            return []
        print("Most popular queries (parameters):")

        for query, count in sorted_queries[:top_n]:
            print(f"Params: {query} | Total Execution for this Query: {count}")

    def get_all_genres_and_years(self): #
        """ Retrieves a list of all available genres and release years of movies... """
        query, params = FilmQueries.get_genres_years()
        self.log_query(query, params)
        try:
            with self.conn.cursor(pymysql.cursors.DictCursor) as cursor:
                cursor.execute(query, params)
                results = cursor.fetchall()
            return results
        except Exception as e:
            logging.error(f"Error retrieving all movies: {e}")
            return []

    def get_films_by_keyword(self, keyword):
        """ Retrieves movies by keyword. """
        query, params = FilmQueries.get_films_by_keyword(keyword)
        self.log_query(query, params)
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                records = cursor.fetchall()
                return records
        except Exception as e:
            logging.error(f"Error retrieving movies by keyword: '{keyword}': {e}")
            return []

    def get_films_by_genre_and_year(self, genre: str, year: int):
        """ Retrieves movies by genre and year. """

        query, params = FilmQueries.get_films_by_genre_and_year(genre, year)
        self.log_query(query, params)
        try:
            with self.get_cursor() as cursor:
                cursor.execute(query, params)
                records = cursor.fetchall()
                return records
        except Exception as e:
            logging.error(f"Error retrieving movies by genre: '{genre}' and year '{year}' : {e}")
            return []
