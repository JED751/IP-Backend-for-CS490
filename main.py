from flask import Flask, jsonify, request
from flask_sqlalchemy import SQLAlchemy #lets use use the MySQL db
from sqlalchemy import text #allows for SQL queries
from flask_cors import CORS #lets us communicate with react
from dotenv import load_dotenv, find_dotenv #for env file used to run backend
import os #used to read env variables
from math import ceil #math ceiling function

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
def top5_films(): #function for getting the top 5 films

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
def top5_actors(): #function for getting the top 5 actors based on movie count

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
def film_details(film_id): #function for getting a film's information

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
        actors = conn.execute(actors_sql, {"film_id": film_id}).mappings().all() #executing query with film_id from URL as parameter
    data = dict(film) #getting the rows and putting them into a dictionary
    data["actors"] = [dict(r) for r in actors] #adding the actors to the dictionary as well
    return jsonify(data) #converting the data found into JSON format

#As a user I want to be able to view the actor’s details and view their top 5 rented films
@app.get("/api/actors/<int:actor_id>") #using the end of the URL as the actor's ID
def actor_details(actor_id): #function for getting an actor's information

    info_sql = text("""  
        SELECT a.actor_id, CONCAT(a.first_name, ' ', a.last_name) AS name, COUNT(DISTINCT fa.film_id) AS films_count
        FROM actor AS a
        LEFT JOIN film_actor AS fa ON fa.actor_id = a.actor_id
        WHERE a.actor_id = :actor_id -- actor_id from URL
        GROUP BY a.actor_id, name;
    """)
    top5_films_sql = text("""
        SELECT f.film_id, f.title, COUNT(r.rental_id) AS rentals_count
        FROM film AS f
        JOIN film_actor AS fa ON fa.film_id = f.film_id
        JOIN inventory AS i ON i.film_id = f.film_id
        JOIN rental AS r ON r.inventory_id = i.inventory_id
        WHERE fa.actor_id = :actor_id -- actor_id from URL
        GROUP BY f.film_id, f.title
        ORDER BY rentals_count DESC
        LIMIT 5;
    """)
    with db.engine.connect() as conn: #connecting to the db
        info = conn.execute(info_sql, {"actor_id": actor_id}).mappings().first() #executing query with actor_id from URL as parameter
        top_films = conn.execute(top5_films_sql, {"actor_id": actor_id}).mappings().all() #executing query with actor_id from URL as parameter
    data = dict(info) #getting the rows and putting them into a dictionary
    data["top_films"] = [dict(r) for r in top_films] #adding the top films to the dictionary as well
    return jsonify(data) #converting the data found into JSON format

#As a user I want to be able to search a film by name of film, name of an actor, or genre of the film
@app.get("/api/films/search") #endpoint for searching for films
def films_search(): #function for searching for a film

    title = request.args.get("title", "", type = str).strip() #getting title from URL, blank if not found
    actor = request.args.get("actor", "", type = str).strip() #getting actor from URL, blank if not found
    genre = request.args.get("genre", "", type = str).strip() #getting genre from URL, blank if not found

    #pagination
    page = max(request.args.get("page", 1, type = int), 1) #ensuring page is not out of bounds
    page_size = min(max(request.args.get("pageSize", 20, type = int), 1), 50) #20 movies at a time per page

    where = [] #array of SQL conditions
    sql_params = {} #used for parameters within SQL queries

    if title: #if a title is found
        where.append("f.title LIKE :title")
        sql_params["title"] = f"%{title}%" #parameter for SQL, partial matching allowed

    if actor: #if an actor is found
        #connecting actor to film_actor for searching
        where.append("""
            EXISTS (
            SELECT 1
            FROM film_actor AS fa
            JOIN actor AS a ON a.actor_id = fa.actor_id
            WHERE fa.film_id = f.film_id
            AND CONCAT(a.first_name, ' ', a.last_name) LIKE :actor)
        """)
        sql_params["actor"] = f"%{actor}%" #parameter for SQL, partial matching allowed

    if genre: #if a genre is found
        #searching if a film matches the current genre
        where.append("""
            EXISTS (
            SELECT 1
            FROM film_category AS fc
            JOIN category AS c ON c.category_id = fc.category_id
            WHERE fc.film_id = f.film_id
            AND c.name LIKE :genre)
        """)
        sql_params["genre"] = f"%{genre}%" #parameter for SQL, partial matching allowed

    where_sql = "WHERE " + " AND ".join(where) if where else "" #combining all search features from user if present
    count = text(f"SELECT COUNT(*) FROM film f {where_sql}") #amount of rows returned from search

    #query to get films with parameters from user, uses offset to show different pages of films
    film_sql = text(f"""
        SELECT f.film_id, f.title, f.release_year, f.rating, f.length, GROUP_CONCAT(DISTINCT c.name ORDER BY c.name SEPARATOR ', ') AS categories
        FROM film AS f
        LEFT JOIN film_category AS fc ON fc.film_id = f.film_id
        LEFT JOIN category AS c ON c.category_id = fc.category_id
        {where_sql}
        GROUP BY f.film_id, f.title, f.release_year, f.rating, f.length
        ORDER BY f.film_id ASC
        LIMIT :limit OFFSET :offset
    """)
    with db.engine.connect() as conn: #connecting to the db
        total = conn.execute(count, sql_params).scalar() #getting number of results
        rows = conn.execute(film_sql, {**sql_params, "limit": page_size, "offset": (page - 1) * page_size}).mappings().all() #displaying a reasonable number of rows

    return jsonify({ #JSON response for frontend to read
        "total": total,
        "totalPages": ceil(total/page_size), #need a whole number so use ceiling
        "page": page,
        "pageSize": page_size,
        "items": [dict(r) for r in rows]
    })

#As a user I want to view a list of all customers (Pref. using pagination)
@app.get("/api/customers") #endpoint for customer page
def customers_list(): #function for returning customers

    #pagination
    page = max(request.args.get("page", 1, type = int), 1) #ensuring page is not out of bounds
    page_size = min(max(request.args.get("pageSize", 20, type = int), 1), 50) #20 customers at a time per page

    count = text("SELECT COUNT(*) FROM customer") #amount of rows returned from search

    #customer information
    sql = text("""
        SELECT c.customer_id, c.store_id, c.first_name, c.last_name, c.email, c.active, c.create_date, a.address, a.address2, a.district, ci.city, co.country,
        (SELECT COUNT(*) FROM rental AS r WHERE r.customer_id = c.customer_id) AS total_rentals,
        (SELECT COUNT(*) FROM rental AS r WHERE r.customer_id = c.customer_id AND r.return_date IS NULL) AS current_rentals,
        (SELECT MAX(rental_date) FROM rental AS r WHERE r.customer_id = c.customer_id) AS last_rental_date
        FROM customer AS c
        JOIN address AS a ON a.address_id = c.address_id
        JOIN city AS ci ON ci.city_id = a.city_id
        JOIN country AS co ON co.country_id = ci.country_id
        ORDER BY c.customer_id
        LIMIT :limit OFFSET :offset
    """)
    with db.engine.connect() as conn: #connecting to the db
        total = conn.execute(count).scalar() #getting number of results
        rows = conn.execute(sql, {"limit": page_size, "offset": (page - 1) * page_size}).mappings().all() #displaying a reasonable number of rows

    return jsonify({ #JSON response for frontend to read
        "total": total,
        "totalPages": ceil(total/page_size), #need a whole number so use ceiling
        "page": page,
        "pageSize": page_size,
        "items": [dict(r) for r in rows]
    })

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True) #running and restarting the server if changes are made on port 127.0.0.1