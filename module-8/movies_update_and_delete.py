# Jacob Achenbach
# Module 8.2

# Import required modules
import mysql.connector
from mysql.connector import errorcode
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database configuration
config = {
    "user": os.getenv("USER"),
    "password": os.getenv("PASSWORD"),
    "host": os.getenv("HOST"),
    "database": os.getenv("DATABASE"),
    "raise_on_warnings": True
}

# Function to display films
def show_films(cursor, title):
    """ Fetch and display films with Genre, Studio, and Director """
    print(f"\n-- {title} --")

    query = """
    SELECT film.film_name AS Name, 
           director.director_name AS Director, 
           genre.genre_name AS Genre, 
           studio.studio_name AS 'Studio Name'
    FROM film
    INNER JOIN genre ON film.genre_id = genre.genre_id
    INNER JOIN studio ON film.studio_id = studio.studio_id
    INNER JOIN director ON film.director_id = director.director_id
    """
    
    cursor.execute(query)
    films = cursor.fetchall()
    
    for film in films:
        print(f"\nName: {film[0]}\nDirector: {film[1]}\nGenre: {film[2]}\nStudio Name: {film[3]}")

try:
    # Connect to MySQL database
    db = mysql.connector.connect(**config)
    cursor = db.cursor()

    print("\n  Database user {} connected to MySQL on host {} with database {}".format(config["user"], config["host"], config["database"]))

    # Display initial film records
    show_films(cursor, "DISPLAYING FILMS BEFORE CHANGES")

    # Insert a new film
    print("\n-- INSERTING A NEW FILM --")
    insert_query = """
    INSERT INTO film (film_name, genre_id, studio_id, film_runtime, director_id)
    VALUES ('Inception', 2, 3, 148, (SELECT director_id FROM director WHERE director_name = 'Christopher Nolan'))
    """
    cursor.execute(insert_query)
    db.commit()

    # Display after insert
    show_films(cursor, "DISPLAYING FILMS AFTER INSERT")

    # Update 'Alien' to be a Horror film
    print("\n-- UPDATING Alien TO BE A HORROR FILM --")
    update_query = """
    UPDATE film 
    SET genre_id = 1
    WHERE film_name = 'Alien'
    """
    cursor.execute(update_query)
    db.commit()

    # Display after update
    show_films(cursor, "DISPLAYING FILMS AFTER UPDATE")

    # Delete 'Gladiator'
    print("\n-- DELETING Gladiator --")
    delete_query = "DELETE FROM film WHERE film_name = 'Gladiator'"
    cursor.execute(delete_query)
    db.commit()

    # Display after delete
    show_films(cursor, "DISPLAYING FILMS AFTER DELETE")

    # Close the connection
    cursor.close()
    db.close()

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("  The supplied username or password are invalid")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("  The specified database does not exist")
    else:
        print(err)
