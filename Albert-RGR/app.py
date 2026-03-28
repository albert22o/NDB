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
    cur.execute("SELECT id, name, brand, price, in_stock FROM stationary_shop.stationaries")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    stationaries = []
    for row in rows:
        stationaries.append(
            {"0": row[0],
             "1": row[1],
             "2": row[2],
             "3": row[3],
             "4": row[4],
             "5": get_avg_price(row[2])
             })
    return render_template("index.html", stationaries=stationaries)

def get_avg_price(brand_name):
    key = f"stationary:{brand_name}:avg_price"
    cached = r.get(key)
    if cached is not None:
        return float(cached)
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM stationary_shop.stationaries WHERE brand = %s", (brand_name,))
    stationaries = cur.fetchall()
    cur.close()
    conn.close()

    if stationaries:
        avg = sum(float(c[1]) for c in stationaries) / len(stationaries)
    else:
        avg = 0

    r.setex(key, 2 * 60 * 60, avg)
    return round(avg, 1)

@app.route("/stationary/new", methods=["GET", "POST"])
def stationary_new():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM stationary_shop.shops")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    shops = []
    for row in rows:
        shops.append(
            {
             "id": row[0],
             "name": row[1],
             })

    if request.method == "POST":
        name = request.form["name"]
        brand = request.form["brand"]
        description = request.form["description"]
        price = request.form["price"]
        stock = request.form["stock"]
        shop_id = request.form["shop_id"]
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO stationary_shop.stationaries (name, brand, description, price, in_stock, shop_id) VALUES (%s, %s, %s, %s, %s, %s)",
            (name, brand, description, price, stock, shop_id)
        )
        conn.commit()
        cur.close()
        conn.close()

        key = f"stationary:{brand}:avg_price"
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT * FROM stationary_shop.stationaries WHERE brand = %s", (brand,))
        stationaries = cur.fetchall()
        cur.close()
        conn.close()

        if stationaries:
            avg = sum(float(c[1]) for c in stationaries) / len(stationaries)
        else:
            avg = 0

        r.setex(key, 2 * 60 * 60, avg)

        return redirect(url_for("index"))
    return render_template("stationary_new.html", shops=shops)

@app.route("/stationary/<int:product_id>", methods=["GET", "POST"])
def stationary_detail(product_id):
    r.zincrby("popular:stationary", 1, product_id)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM stationary_shop.stationaries WHERE id = %s", (product_id,))
    stationary = cur.fetchone()
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
        r.delete(f"stationary:{product_id}:avg_rating")
        return redirect(url_for("stationary_detail", product_id=product_id))

    comments = list(comments_collection.find({"product_id": product_id}))
    return render_template("stationary_detail.html", stationary=stationary, comments=comments)

@app.route("/popular")
def popular():
    top = r.zrevrange("popular:stationary", 0, 3, withscores=True)

    badtop = r.zrange("popular:stationary", 0, 3, withscores=True)

    stationaries = []
    badstationaries = []
    conn = get_connection()
    cur = conn.cursor()
    for car_id, views in top:
        cur.execute("SELECT id, name, brand, price FROM stationary_shop.stationaries WHERE id = %s", (int(car_id),))
        row = cur.fetchone()
        if row:
            stationaries.append({"id": row[0], "name": row[1], "brand": row[2],
                         "price": row[3], "views": int(views)})

    for car_id, views in badtop:
        cur.execute("SELECT id, name, brand, price FROM stationary_shop.stationaries WHERE id = %s", (int(car_id),))
        row = cur.fetchone()
        if row:
            badstationaries.append({"id": row[0], "name": row[1], "brand": row[2],
                         "price": row[3], "views": int(views)})

    cur.close()
    conn.close()
    return render_template("popular.html", stationaries=stationaries, badstationaries=badstationaries)

def get_avg_rating(product_id):
    key = f"stationary:{product_id}:avg_rating"
    cached = r.get(key)
    if cached is not None:
        return float(cached)

    pipeline = [
        {"$match": {"product_id": product_id}},
        {"$group": {
            "_id": None,
            "avg": {"$avg": "$rating"}
        }}
    ]

    result = list(comments_collection.aggregate(pipeline))

    if result and result[0].get("avg") is not None:
        avg = result[0]["avg"]
    else:
        avg = 0

    r.setex(key, 3600, avg)
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
        cur.execute("SELECT id, name, price FROM stationary_shop.stationaries WHERE name ILIKE %s", (f"%{name_query}%",))
        stationaries = cur.fetchall()
        cur.close()
        conn.close()

        for stationary in stationaries:
            car_comments = list(comments_collection.find({"product_id": stationary[0]}))
            if car_comments:
                avg_rating = get_avg_rating(stationary[0]);
            else:
                avg_rating = 0
            if avg_rating >= min_rating:
                results.append({"id": stationary[0], "name": stationary[1], "price": stationary[2], "avg_rating": round(avg_rating, 1)})

    return render_template("search.html", results=results, searched=searched)

@app.route("/shop/new", methods=["GET", "POST"])
def shop_new():
    if request.method == "POST":
        name = request.form["name"]
        address = request.form["address"]
        registration_date = request.form["registration_date"]
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO stationary_shop.shops (name, address, registration_date) VALUES (%s, %s, %s)",
            (name, address, registration_date)
        )
        conn.commit()
        cur.close()
        conn.close()
        return redirect(url_for("shops"))
    return render_template("shop_new.html")
@app.route("/shops")
def shops():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT name, address, registration_date FROM stationary_shop.shops")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    shops = []
    for row in rows:
        shops.append(
            {"0": row[0],
             "1": row[1],
             "2": row[2]
             })

    return render_template("shops.html", shops=shops)

@app.route("/order/new", methods=["GET", "POST"])
def order_new():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT id, name FROM stationary_shop.stationaries")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    stationaries = []
    for row in rows:
        stationaries.append(
            {
             "id": row[0],
             "name": row[1],
             })

    if request.method == "POST":
        stationary_id = request.form["stationary_id"]
        date_of_order = request.form["date_of_order"]
        amount = request.form["amount"]
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO stationary_shop.orders (stationary_id, date_of_order, amount) VALUES (%s, %s, %s)",
            (stationary_id, date_of_order, amount)
        )
        conn.commit()
        cur.close()
        conn.close()
        return redirect(url_for("orders"))
    return render_template("order_new.html", stationaries = stationaries)
@app.route("/orders")
def orders():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT s.name, o.date_of_order, o.amount FROM stationary_shop.orders o"
                " join stationary_shop.stationaries s on (s.id = o.stationary_id)")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    orders = []
    for row in rows:
        orders.append(
            {
             "0": row[0],
             "1": row[1],
             "2": row[2]
             })

    return render_template("orders.html", orders=orders)


if __name__ == "__main__":
    app.run(debug=True)