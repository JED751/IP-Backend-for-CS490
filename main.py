from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy #lets use use the MySQL db
from sqlalchemy import text #allows for SQL queries
from flask_cors import CORS #lets us communicate with react
from dotenv import load_dotenv, find_dotenv #for env file used to run backend
import os #used to read env variables

load_dotenv(find_dotenv()) #connection to mySQL db in env

app = Flask(__name__) #creating flask application object

CORS(app, resources={
    r"/api/*": {
        "origins": [ #the ports that flask will be able to connect to
            "http://localhost:5173", 
            "http://127.0.0.1:5173",
            "http://localhost:3000", #3000 initially used for tutorial and 3001 for actual project
            "http://127.0.0.1:3000",
            "http://localhost:3001",
            "http://127.0.0.1:3001",
            ]}})

db_uri = os.getenv("DATABASE_URL") #getting the db connection from env
app.config["SQLALCHEMY_DATABASE_URI"] = db_uri
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
db = SQLAlchemy(app) #db object

#As a user I want to view top 5 rented films of all time
@app.get("/api/films/top5") #route to homepage
def top5_films(): #function for getting the top 5 films, used query written for milestone 1 question 6
    sql = text("""
        SELECT f.film_id, f.title, COUNT(r.rental_id) AS rentals_count
        FROM film f
        JOIN inventory i ON i.film_id = f.film_id
        JOIN rental r ON r.inventory_id = i.inventory_id
        GROUP BY f.film_id, f.title
        ORDER BY rentals_count DESC
        LIMIT 5;
    """)
    with db.engine.connect() as conn: #connecting to the db
        rows = conn.execute(sql).mappings().all() #executing the query
    return jsonify([dict(r) for r in rows]) #getting the rows and putting them into a dictionary

#As a user I want to be able to view top 5 actors that are part of films I have in the store
@app.get("/api/actors/top5")
def top5_actors():
    sql = text("""
        SELECT a.actor_id, CONCAT(a.first_name, ' ', a.last_name) AS name, COUNT(DISTINCT fa.film_id) AS films_count
        FROM actor a
        JOIN film_actor fa ON fa.actor_id = a.actor_id
        GROUP BY a.actor_id, name
        ORDER BY films_count DESC, name ASC
        LIMIT 5;
    """)
    with db.engine.connect() as conn: #connecting to the db
        rows = conn.execute(sql).mappings().all() #executing the query
    return jsonify([dict(r) for r in rows]) #getting the rows and putting them into a dictionary

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True) #running and restarting the server if changes are made on port 127.0.0.1