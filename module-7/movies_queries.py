# Jacob Achenbach
# Module 7.2

# Import required modules
import mysql.connector  # MySQL connector
from mysql.connector import errorcode
from dotenv import load_dotenv  # Load environment variables
import os  # OS module to access .env variables

# Load environment variables from .env file
load_dotenv()

# Database configuration
config = {
    "user": os.getenv("USER"),
    "password": os.getenv("PASSWORD"),
    "host": os.getenv("HOST"),
    "database": os.getenv("DATABASE"),
    "raise_on_warnings": True
}

try:
    # Connect to MySQL database
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    print("\n  Database user {} connected to MySQL on host {} with database {}".format(config["user"], config["host"], config["database"]))

    # Query 1: Display Studio Records (Ensure distinct values)
    print("\n-- DISPLAYING Studio RECORDS --")
    cursor.execute("SELECT DISTINCT studio_id, studio_name FROM studio")  
    studios = cursor.fetchall()
    for studio_id, studio_name in studios:
        print(f"\nStudio ID: {studio_id}\nStudio Name: {studio_name}")

    # Query 2: Display Genre Records (Ensure distinct values)
    print("\n-- DISPLAYING Genre RECORDS --")
    cursor.execute("SELECT DISTINCT genre_id, genre_name FROM genre")  
    genres = cursor.fetchall()
    for genre_id, genre_name in genres:
        print(f"\nGenre ID: {genre_id}\nGenre Name: {genre_name}")

    # Query 3: Display Movies with Runtime < 120 minutes
    print("\n-- DISPLAYING Short Film RECORDS --")
    cursor.execute("SELECT DISTINCT film_name, film_runtime FROM film WHERE film_runtime < 120")  
    short_films = cursor.fetchall()
    for film_name, runtime in short_films:
        print(f"\nFilm Name: {film_name}\nRuntime: {runtime}")

    # Query 4: Display Directors with Their Movies (Check if director table exists)
    try:
        print("\n-- DISPLAYING Director RECORDS in Order --")
        cursor.execute(
            "SELECT film.film_name, director.director_name FROM film "
            "INNER JOIN director ON film.director_id = director.director_id "
            "ORDER BY director.director_name"
        )
        films_by_director = cursor.fetchall()
        for film_name, director_name in films_by_director:
            print(f"\nFilm Name: {film_name}\nDirector: {director_name}")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_NO_SUCH_TABLE:
            print("\nError: The 'director' table does not exist in the database. Please check your schema.")

    # Close the connection
    cursor.close()
    db.close()

except mysql.connector.Error as err:
    # Handle errors
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("  The supplied username or password are invalid")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("  The specified database does not exist")
    else:
        print(err)
