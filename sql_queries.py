class FilmQueries:
    """ Class containing SQL queries for working with movies in the sakila database. """
    @staticmethod
    def get_genres_years():
        return (
    """
            SELECT DISTINCT c.name AS genre, f.release_year
            FROM sakila.film AS f
            JOIN sakila.film_category AS fc ON f.film_id = fc.film_id
            JOIN sakila.category AS c ON c.category_id = fc.category_id
            ORDER BY c.name, f.release_year;
    """,
    ()
        )

    @staticmethod
    def get_films_by_keyword(keyword):
        """ Forms a query to search for movies by a keyword in the title or description. """
        return (
    """
            SELECT title, description, release_year 
            FROM sakila.film 
            WHERE title LIKE %s OR description LIKE %s
            LIMIT 10;
        """,
            ('%' + keyword + '%', '%' + keyword + '%')
        )
    @staticmethod
    def get_films_by_genre_and_year(genre, year):
        """ Forms a query to search for movies by genre and release year. """
        return (
    """
            SELECT f.title, f.release_year, c.name 
            FROM sakila.film AS f 
            JOIN sakila.film_category AS fc ON f.film_id = fc.film_id 
            JOIN sakila.category AS c ON c.category_id = fc.category_id 
            WHERE c.name = %s AND f.release_year = %s;
        """,
            (genre, year)
        )

