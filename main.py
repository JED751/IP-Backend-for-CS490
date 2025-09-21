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
               FROM film AS f
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
               FROM actor AS a
               JOIN film_actor fa ON fa.actor_id = a.actor_id
               GROUP BY a.actor_id, name
               ORDER BY films_count DESC, name ASC
               LIMIT 5;
    """)
    with db.engine.connect() as conn: #connecting to the db
        rows = conn.execute(sql).mappings().all() #executing the query
    return jsonify([dict(r) for r in rows]) #getting the rows and putting them into a dictionary

#As a user I want to be able to click on any of the top 5 films and view its details
@app.get("/api/films/<int:film_id>") #using the end of the URL as the film's ID
def film_details(film_id):
    sql = text("""
               -- getting film information
               SELECT f.film_id AS id, f.title, f.description, f.release_year, GROUP_CONCAT(DISTINCT c.name ORDER BY c.name SEPARATOR ', ') AS categories,
               l.name AS language, f.rental_duration, f.rental_rate, f.length AS duration, f.replacement_cost, f.rating, f.special_features, f.last_update,
               (SELECT COUNT(*)
               FROM rental AS r
               JOIN inventory i ON i.inventory_id = r.inventory_id
               WHERE i.film_id = f.film_id) AS rentals_count
               FROM film AS f
               JOIN language AS l ON l.language_id = f.language_id
               LEFT JOIN film_category AS fc ON fc.film_id   = f.film_id
               LEFT JOIN category AS c ON c.category_id = fc.category_id
               WHERE f.film_id = :film_id -- film_id from URL
               GROUP BY
               f.film_id, f.title, f.description, f.release_year, l.name, f.rental_duration, f.rental_rate, f.length, f.replacement_cost, f.rating, f.special_features, f.last_update;

    """)
    actors_sql = text("""
               -- getting actor information to display actors involved when clicking on a film
               SELECT a.actor_id, CONCAT(a.first_name, ' ', a.last_name) AS name
               FROM actor AS a
               JOIN film_actor fa ON fa.actor_id = a.actor_id
               WHERE fa.film_id = :film_id -- film_id from URL
               ORDER BY name;
    """)
    with db.engine.connect() as conn: #connecting to the db
        film = conn.execute(sql, {"film_id": film_id}).mappings().first() #executing query with film_id from URL as parameter
        actors = conn.execute(actors_sql, {"film_id": film_id}).mappings().all()
    data = dict(film) #getting the rows and putting them into a dictionary
    data["actors"] = [dict(a) for a in actors] #adding the actors to the dictionary as well
    return jsonify(data) #converting the data found into JSON format

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True) #running and restarting the server if changes are made on port 127.0.0.1