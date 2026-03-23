from flask import Flask, render_template, request, redirect, url_for
from models.db_postgres import get_connection
from models.db_mongo import reviews_collection, sellers_collection
from models.db_redis import r
from datetime import datetime

app = Flask(__name__)

@app.route('/')
def home():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT building_id, area, price, sold, district, seller_id FROM agents.buildings")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    buildings = []
    for row in rows:
        buildings.append({
            "building_id": row[0],
            "area": row[1],
            "price": row[2],
            "sold": row[3],
            "district": row[4],
            "seller_id": row[5],
            "avg_rating": get_avg_rating(row[0]),
        })

    return render_template("index.html", buildings=buildings)


@app.route("/building/<int:building_id>", methods=["GET", "POST"])
def building_detail(building_id):
    r.zincrby("popular:buildings", 1, building_id)

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "SELECT building_id, area, price, sold, district, seller_id FROM agents.buildings WHERE building_id = %s",
        (building_id,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row is None:
        return "Объект не найден", 404

    building = {
        "building_id": row[0],
        "area": row[1],
        "price": row[2],
        "sold": row[3],
        "district": row[4],
        "seller_id": row[5],
    }

    if request.method == "POST":
        review = {
            "building_id": building_id,
            "author": request.form.get("author") or "Аноним",
            "text": request.form["text"],
            "rating": int(request.form["rating"]),
            "created_at": datetime.now(),
        }
        reviews_collection.insert_one(review)
        r.delete(f"building:{building_id}:avg_rating")
        return redirect(url_for("building_detail", building_id=building_id))

    reviews = list(reviews_collection.find({"building_id": building_id}))
    return render_template("building_detail.html", building=building, reviews=reviews)


@app.route("/sell", methods=["GET", "POST"])
def add_building():
    if request.method == "POST":
        area = request.form["area"]
        price = request.form["price"]
        sold = True if request.form.get("sold") else False
        district = request.form["district"]
        seller_id = request.form["seller_id"]

        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO agents.buildings (area, price, sold, district, seller_id)
               VALUES (%s, %s, %s, %s, %s)""",
            (area, price, sold, district, seller_id)
        )
        conn.commit()
        cur.close()
        conn.close()

        r.delete("cache:popular_districts")

        return redirect(url_for("home"))

    return render_template("new_sell.html")


@app.route("/add_seller", methods=["GET", "POST"])
def add_seller():
    if request.method == "POST":
        full_name = request.form["fullName"]
        date_of_birth = request.form["dateOfBirth"]
        date_of_registration = request.form.get("dateOfRegistration") or None

        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO agents.sellers ("fullName", "dateOfBirth", "dateOfRegistration")
               VALUES (%s, %s, %s)""",
            (full_name, date_of_birth, date_of_registration)
        )
        conn.commit()
        cur.close()
        conn.close()

        sellers_collection.insert_one({
            "fullName": full_name,
            "dateOfBirth": date_of_birth,
            "dateOfRegistration": date_of_registration,
            "created_at": datetime.now(),
        })

        return redirect(url_for("home"))

    return render_template("new_seller.html")


@app.route("/popular", methods=["GET"])
def get_popular_districts():
    import json
    cache_key = "cache:popular_districts"
    cached = r.get(cache_key)

    if cached:
        districts = json.loads(cached)
    else:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT b.district, COUNT(o.order_id) AS total_orders
            FROM agents.orders o
            JOIN agents.buildings b ON o.building_id = b.building_id
            GROUP BY b.district
            ORDER BY total_orders DESC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        districts = [{"district": row[0], "total_orders": row[1]} for row in rows]
        # TTL = 10 минут
        r.setex(cache_key, 10 * 60, json.dumps(districts, ensure_ascii=False))

    return render_template("popular.html", districts=districts)

@app.route("/trending")
def trending():
    top = r.zrevrange("popular:buildings", 0, 4, withscores=True)

    buildings = []
    conn = get_connection()
    cur = conn.cursor()
    for building_id, views in top:
        cur.execute(
            "SELECT building_id, area, price, district FROM agents.buildings WHERE building_id = %s",
            (int(building_id),)
        )
        row = cur.fetchone()
        if row:
            buildings.append({
                "building_id": row[0],
                "area": row[1],
                "price": row[2],
                "district": row[3],
                "views": int(views),
            })
    cur.close()
    conn.close()

    return render_template("trending.html", buildings=buildings)



@app.route("/orders", methods=["GET", "POST"])
def get_orders():
    conn = get_connection()
    cur = conn.cursor()

    if request.method == "POST":
        seller_id = request.form["seller_id"]
        building_id = request.form["building_id"]
        actual_sell_price = request.form["actual_sell_price"]

        cur.execute(
            """INSERT INTO agents.orders (seller_id, building_id, actual_sell_price)
               VALUES (%s, %s, %s)""",
            (seller_id, building_id, actual_sell_price)
        )
        cur.execute(
            "UPDATE agents.buildings SET sold = TRUE WHERE building_id = %s",
            (building_id,)
        )
        conn.commit()

        r.delete("cache:popular_districts")

    cur.execute("""
        SELECT o.order_id, s."fullName", b.district, b.area,
               o.actual_sell_price, o."orderTime"
        FROM agents.orders o
        JOIN agents.sellers s ON o.seller_id = s.seller_id
        JOIN  agents.buildings b ON o.building_id = b.building_id
        ORDER BY o."orderTime" DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    orders = [
        {
            "order_id": row[0],
            "seller_name": row[1],
            "district": row[2],
            "area": row[3],
            "actual_sell_price": row[4],
            "order_time": row[5],
        }
        for row in rows
    ]

    return render_template("orders.html", orders=orders)



def get_avg_rating(building_id):
    key = f"building:{building_id}:avg_rating"
    cached = r.get(key)
    if cached is not None:
        return float(cached)

    pipeline = [
        {"$match": {"building_id": building_id}},
        {"$group": {"_id": "$building_id", "avg": {"$avg": "$rating"}}},
    ]
    result = list(reviews_collection.aggregate(pipeline))
    avg = result[0]["avg"] if result else 0

    r.setex(key, 3600, avg)
    return round(avg, 1)

@app.route("/sellers")
def list_sellers():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute('SELECT seller_id, "fullName", "dateOfBirth", "dateOfRegistration" FROM agents.sellers')
    rows = cur.fetchall()
    cur.close()
    conn.close()

    sellers = [
        {
            "seller_id": row[0],
            "full_name": row[1],
            "dob": row[2],
            "reg_date": row[3]
        }
        for row in rows
    ]
    return render_template("sellers.html", sellers=sellers)

if __name__ == '__main__':
    app.run(debug=True)