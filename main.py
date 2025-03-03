import pymysql

from db_config import DBConfig
from query_manager import QueryHandler

# Очистка файла query_log.log перед запуском
#if os.path.exists("query_log.log"):
#    os.remove("query_log.log")


def task1(dbconfig, keyword):
    query_handler = QueryHandler(dbconfig)
    try:
        films = query_handler.get_films_by_keyword(keyword)
        if not films:
            print("No movies found.")
            return
        for row in films:
            print(f"Title: {row.get('title')}")
            print(f"Description: {row.get('description')}")
            print(f"Year: {row.get('release_year')}")
            print("-" * 30)
    except pymysql.Error as e:
        print("SQLError", e)
    except Exception as e:
        print("Error", e)
    finally:
        query_handler.close()

def task2(dbconfig, genre, year):
    query_handler = QueryHandler(dbconfig)
    try:
        films = query_handler.get_films_by_genre_and_year(genre, year)
        if not films:
            print("No movies found.")
            return
        for row in films:
            print(f"Title: {row.get('title')}")
            print(f"Genre: {row.get('name')}")
            print(f"Year: {row.get('release_year')}")
            print("-" * 30)
    except pymysql.Error as e:
        print("SQLError", e)
    except Exception as e:
        print("Error", e)
    finally:
        query_handler.close()

def task3(dbconfig):
    query_handler = QueryHandler(dbconfig)
    try:
        results = query_handler.get_all_genres_and_years()
        if not results:
            print("Failed to load genres and years.")
            return

        grouped = {}
        for row in results:
            genre = row.get('genre')
            year = row.get('release_year')
            if genre not in grouped:
                grouped[genre] = set()
            grouped[genre].add(year)


        print("Available genres and years: ")
        for genre, years in grouped.items():
            print(f"Genre: {genre}")
            print("Years: " + ", ".join(map(str, sorted(years))))
            print("-" * 30)

    except pymysql.Error as e:
        print("SQLError", e)
    except Exception as e:
        print("Error", e)
    finally:
        query_handler.close()

if __name__ == "__main__":
    dbconfig = DBConfig()
    print(dbconfig.get_dbconfig())
    query_handler = QueryHandler(dbconfig.get_dbconfig())
    while True:
        print("\nSelect an action: ")
        print("1. Search by keyword")
        print("2. Search by genre and year")
        print("3. Display all genres and years")
        print("4. Show popular queries")
        print("5. Exit")

        choice = input("Enter the action number: ")

        if choice == "1":
            keyword = input("Enter the keyword to search for movies: ")
            task1(dbconfig.get_dbconfig(), keyword)

        elif choice == "2":
            genre = input("Enter the genre: ")
            try:
                year = int(input("Enter the year: "))
                task2(dbconfig.get_dbconfig(), genre, year)
            except ValueError:
                print("Error: The year must be a number.")

        elif choice == "3":
            task3(dbconfig.get_dbconfig())

        elif choice == "4":
            query_handler.get_popular_queries()

        elif choice == '5':
            print("Exiting the program.")
            break

        else:
            print("invalid choice. Please try again.")