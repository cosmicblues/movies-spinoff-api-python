import os
import json
import psycopg2
import requests
from psycopg2.extensions import AsIs

mot_passe = os.environ.get('pg_psw')
conn = psycopg2.connect(
    database = "postgres",
    user = "postgres",
    host = "127.0.0.1",
    password = mot_passe
    )

def createbdd ():
    conn.autocommit = True

    cursor = conn.cursor()

    sql = '''CREATE database python_data_api'''

    cursor.execute(sql)
    conn.commit()
    print("Base de données créée avec succès !")

def ouvrir_connection(nom_bdd, utilisateur, mot_passe, host='localhost', port=5432):
    try:
        conn = psycopg2.connect(dbname=nom_bdd, user=utilisateur, password=mot_passe, host=host, port=5432)
    except psycopg2.Error as e:
        print("Erreur lors de la connection à la base de données")
        print(e)
        return None
    # On force autocommit (non applicable ds SQLite3)
    conn.set_session(autocommit=True)
    return conn

sql_supprimer_table_movies = """
    DROP TABLE IF EXISTS ratings;
"""

sql_creer_table_movies = """
    CREATE TABLE IF NOT EXISTS movies (
        Title VARCHAR(255) NOT NULL,
        Year VARCHAR(255),
        Rated VARCHAR(255),
        Released VARCHAR(255),
        Runtime VARCHAR(255),
        Genre VARCHAR(255),
        Director VARCHAR(255),
        Writer VARCHAR(255),
        Actors VARCHAR(255),
        Plot VARCHAR(255),
        Language VARCHAR(255),
        Country VARCHAR(255),
        Awards VARCHAR(255),
        Poster VARCHAR(255),
        Ratings VARCHAR(255),
        Metascore VARCHAR(255),
        imdbRating VARCHAR(255),
        imdbVotes VARCHAR(255),
        imdbID VARCHAR(255),
        Type VARCHAR(255),
        DVD VARCHAR(255),
        BoxOffice VARCHAR(255),
        Production VARCHAR(255),
        Website VARCHAR(255),
        Response VARCHAR(255)
        )
        """

sql_creer_table_ratings = """ 
    CREATE TABLE ratings (
        Source VARCHAR(255),
        Value VARCHAR(255)
        );
    """

sql_inserer_movies = """
    COPY movies 
    (movieId, title, genres)
    FROM 'C:\\Users\\Public\\Documents\\movies-small\\movies.csv'
    WITH CSV DELIMITER ','
    QUOTE '"'
    HEADER;
"""

sql_inserer_ratings = """
    COPY ratings 
    (userId, movieId, rating, timestamp)
    FROM 'C:\\Users\\Public\\Documents\\movies-small\\ratings.csv'
    WITH CSV DELIMITER ','
    QUOTE '"'
    HEADER;
"""

def supprimer_table(conn, sql_suppression_table):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_suppression_table)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de la suppression de la table")
        print(e)
        return
    cursor.close()
    print("La table a été supprimée avec succès")

    
def creer_table(conn, sql_creation_table):
    ma_base_donnees = "python_data_api"
    utilisateur = "postgres"
    mot_passe = os.environ.get('pg_psw')
    conn = ouvrir_connection(ma_base_donnees, utilisateur, mot_passe)
    try:
        cursor = conn.cursor()
        cursor.execute(sql_creation_table)
        conn.commit()
    except psycopg2.Error as e:
        print("Erreur lors de la création de la table")
        print(e)
        return
    cursor.close()
    print("La table a été crée avec succès")

def inserer_donnees(conn, request):
    ma_base_donnees = "python_data_api"
    utilisateur = "postgres"
    mot_passe = os.environ.get('pg_psw')
    conn = ouvrir_connection(ma_base_donnees, utilisateur, mot_passe)

    response = requests.get(request)
    response_dict = response.json()
    del response_dict["Ratings"]
    
    columns = response_dict.keys()
    values = [response_dict[column] for column in columns]
    
    cursor = conn.cursor()
    columns_name = AsIs(','.join(columns))
    print(columns_name)
    insert_statement = f'insert into movies ({columns_name}) values {tuple(values)}'
    #print(insert_statement)
    cursor.execute(insert_statement)
    conn.commit()

#createbdd()
#creer_table(conn, sql_creer_table_movies)
inserer_donnees(conn, "http://www.omdbapi.com/?t=Batman&page=1&apikey=cfa14655")

