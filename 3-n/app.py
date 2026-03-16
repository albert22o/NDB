from flask import Flask, render_template, request, redirect, url_for
from models.db_postgres import get_connection
from models.db_mongo import comments_collection
from datetime import datetime

app = Flask(__name__)

@app.route("/")
def index():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name, brand, price, stock FROM cars.cars")
    cars = cur.fetchall()
    cur.close()
    conn.close()
    return render_template("index.html", cars=cars)


@app.route("/cars/new", methods=["GET", "POST"])
def car_new():
    if request.method == "POST":
        name = request.form["name"]
        brand = request.form["brand"]
        description = request.form["description"]
        price = request.form["price"]
        stock = request.form["stock"]
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO cars.cars (name, brand, description, price, stock) VALUES (%s, %s, %s, %s, %s)",
            (name, brand, description, price, stock)
        )
        conn.commit()
        cur.close()
        conn.close()
        return redirect(url_for("index"))
    return render_template("car_new.html")

@app.route("/cars/<int:product_id>", methods=["GET", "POST"])
def car_detail(product_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM cars.cars WHERE id = %s", (product_id,))
    car = cur.fetchone()
    cur.close()
    conn.close()

    if request.method == "POST":
        comment = {
            "product_id": product_id,
            "author": request.form.get("author") or "Аноним",
            "text": request.form["text"],
            "rating": int(request.form["rating"]),
            "created_at": datetime.now()
        }
        comments_collection.insert_one(comment)
        return redirect(url_for("car_detail", product_id=product_id))

    comments = list(comments_collection.find({"product_id": product_id}))
    return render_template("car_detail.html", car=car, comments=comments)

@app.route("/search", methods=["GET", "POST"])
def search():
    results = []
    searched = False
    if request.method == "POST":
        searched = True
        name_query = request.form.get("name", "")
        min_rating = int(request.form.get("min_rating", 0))

        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT id, name, price FROM cars.cars WHERE name ILIKE %s", (f"%{name_query}%",))
        cars = cur.fetchall()
        cur.close()
        conn.close()

        for car in cars:
            car_comments = list(comments_collection.find({"product_id": car[0]}))
            if car_comments:
                avg_rating = sum(c["rating"] for c in car_comments) / len(car_comments)
            else:
                avg_rating = 0
            if avg_rating >= min_rating:
                results.append({"id": car[0], "name": car[1], "price": car[2], "avg_rating": round(avg_rating, 1)})

    return render_template("search.html", results=results, searched=searched)

if __name__ == "__main__":
    app.run(debug=True)