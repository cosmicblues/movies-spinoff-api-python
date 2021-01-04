import json
import psycopg2

conn = psycopg2.connect(
    database = "postgres",
    user = "postgres",
    host = "127.0.0.1",
    password = "Th302k18"
)

conn.autocommit = True

cursor = conn.cursor()

sql = '''CREATE database python_data_api'''

cursor.execute(sql)
conn.commit()
print("Base de données créée avec succès !")

conn.close()
   

  