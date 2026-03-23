from flask import Flask, render_template, request, redirect, url_for
from models.db_postgres import get_connection
from models.db_mongo import comments_collection
from datetime import datetime

from models.db_redis import r

app = Flask(__name__)

@app.route("/")
def index():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name, brand, price, stock, roof FROM cars.cars")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    cars = []
    for row in rows:
        cars.append(
            {"0": row[0],
             "1": row[1],
             "2": row[2],
             "3": row[3],
             "4": row[4],
             "5": row[5],
             "6": get_avg_price(row[2])
             })


    return render_template("index.html", cars=cars)


@app.route("/cars/new", methods=["GET", "POST"])
def car_new():
    if request.method == "POST":
        name = request.form["name"]
        brand = request.form["brand"]
        description = request.form["description"]
        price = request.form["price"]
        stock = request.form["stock"]
        has_roof = True if request.form.get("hasroof") == "on" else False
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO cars.cars (name, brand, description, price, stock, roof) VALUES (%s, %s, %s, %s, %s, %s)",
            (name, brand, description, price, stock, has_roof)
        )
        conn.commit()
        cur.close()
        conn.close()

        key = f"car:{brand}:avg_price"
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM cars.cars WHERE brand = %s", (brand,))
        cars = cur.fetchall()
        cur.close()
        conn.close()

        if cars:
            avg = sum(float(c[4]) for c in cars) / len(cars)
        else:
            avg = 0

        r.setex(key, 2 * 60 * 60, avg)

        return redirect(url_for("index"))
    return render_template("car_new.html")

@app.route("/cars/<int:product_id>", methods=["GET", "POST"])
def car_detail(product_id):
    r.zincrby("popular:cars", 1, product_id)

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
        r.delete(f"car:{product_id}:avg_rating")
        return redirect(url_for("car_detail", product_id=product_id))

    comments = list(comments_collection.find({"product_id": product_id}))
    return render_template("car_detail.html", car=car, comments=comments)


@app.route("/popular")
def popular():
    top = r.zrevrange("popular:cars", 0, 3, withscores=True)

    badtop = r.zrange("popular:cars", 0, 3, withscores=True)

    cars = []
    badcars = []
    conn = get_connection()
    cur = conn.cursor()
    for car_id, views in top:
        cur.execute("SELECT id, name, brand, price FROM cars.cars WHERE id = %s", (int(car_id),))
        row = cur.fetchone()
        if row:
            cars.append({"id": row[0], "name": row[1], "brand": row[2],
                         "price": row[3], "views": int(views)})

    for car_id, views in badtop:
        cur.execute("SELECT id, name, brand, price FROM cars.cars WHERE id = %s", (int(car_id),))
        row = cur.fetchone()
        if row:
            badcars.append({"id": row[0], "name": row[1], "brand": row[2],
                         "price": row[3], "views": int(views)})

    cur.close()
    conn.close()
    return render_template("popular.html", cars=cars, badcars=badcars)

def get_avg_rating(product_id):
    key = f"car:{product_id}:avg_rating"
    cached = r.get(key)
    if cached is not None:
        return float(cached)

    comments = list(comments_collection.find({"product_id": product_id}))
    if comments:
        avg = sum(c["rating"] for c in comments) / len(comments)
    else:
        avg = 0

    r.setex(key, 3600, avg)
    return round(avg, 1)

def get_avg_price(brand_name):
    key = f"car:{brand_name}:avg_price"
    cached = r.get(key)
    if cached is not None:
        return float(cached)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM cars.cars WHERE brand = %s", (brand_name,))
    cars = cur.fetchall()
    cur.close()
    conn.close()

    if cars:
        avg = sum(float(c[4]) for c in cars) / len(cars)
    else:
        avg = 0

    r.setex(key, 2 * 60 * 60, avg)
    return round(avg, 1)

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
                avg_rating = get_avg_rating(car[0]);
            else:
                avg_rating = 0
            if avg_rating >= min_rating:
                results.append({"id": car[0], "name": car[1], "price": car[2], "avg_rating": round(avg_rating, 1)})

    return render_template("search.html", results=results, searched=searched)

if __name__ == "__main__":
    app.run(debug=True)