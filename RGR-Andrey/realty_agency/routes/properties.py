from flask import Blueprint, render_template, request, redirect, url_for, jsonify, abort
from db import get_pg, pg_cursor, get_redis, get_mongo_db
import json
from datetime import datetime

properties_bp = Blueprint('properties', __name__)


# ── GET /properties ──────────────────────────────────────────────────────────
@properties_bp.route('/')
def list_properties():
    """
    Список объектов с JOIN на agents и фильтрами.
    GET /properties?type=apartment&status=available
    Входные: type (str, опц.), status (str, опц.)
    Выходные: список PropertyEntity
    """
    prop_type = request.args.get('type', '')
    status    = request.args.get('status', '')

    # Кеш для популярных фильтров
    cache_key = f'props:{prop_type}:{status}'
    r = get_redis()
    cached = r.get(cache_key)
    if cached:
        props = json.loads(cached)
    else:
        cur = pg_cursor()
        query = """
            SELECT p.id, p.title, p.address, p.type, p.area_sqm,
                   p.price, p.status, p.listed_at, p.description,
                   a.full_name AS agent_name, a.phone AS agent_phone
            FROM   properties p
            JOIN   agents a ON a.id = p.agent_id
            WHERE  (%s = '' OR p.type = %s)
              AND  (%s = '' OR p.status = %s)
            ORDER  BY p.listed_at DESC
        """
        cur.execute(query, (prop_type, prop_type, status, status))
        props = [dict(r) for r in cur.fetchall()]
        # Кеш 30 секунд (TTL)
        r.setex(cache_key, 30, json.dumps(props, default=str))

    return render_template('properties.html', properties=props,
                           filter_type=prop_type, filter_status=status)


# ── GET /properties/<id> ─────────────────────────────────────────────────────
@properties_bp.route('/<int:prop_id>')
def property_detail(prop_id):
    """
    Карточка объекта.
    GET /properties/<prop_id>
    Входные: prop_id – ID объекта (path)
    Выходные: PropertyDetailEntity + отзывы из MongoDB
    """
    cur = pg_cursor()
    cur.execute("""
        SELECT p.*, a.full_name AS agent_name, a.phone AS agent_phone,
               a.email AS agent_email
        FROM   properties p
        JOIN   agents a ON a.id = p.agent_id
        WHERE  p.id = %s
    """, (prop_id,))
    prop = cur.fetchone()
    if not prop:
        abort(404)

    # Отзывы из MongoDB
    mongo = get_mongo_db()
    reviews = list(mongo.reviews.find(
        {'property_id': prop_id},
        {'_id': 0}
    ).sort('created_at', -1).limit(10))

    return render_template('property_detail.html', prop=dict(prop), reviews=reviews)


# ── POST /properties ─────────────────────────────────────────────────────────
@properties_bp.route('/', methods=['POST'])
def add_property():
    """
    Добавить объект недвижимости.
    POST /properties
    Входные (form): title, address, type, area_sqm, price, agent_id, description
    Выходные: redirect на /properties
    """
    d = request.form
    cur = pg_cursor()
    cur.execute("""
        INSERT INTO properties (title, address, type, area_sqm, price, agent_id, description)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (d['title'], d['address'], d['type'],
          d['area_sqm'], d['price'], d['agent_id'], d.get('description', '')))
    # Сбрасываем кеш
    r = get_redis()
    for key in r.scan_iter('props:*'):
        r.delete(key)
    r.delete('stats:main')
    return redirect(url_for('properties.list_properties'))


# ── POST /properties/<id>/review ─────────────────────────────────────────────
@properties_bp.route('/<int:prop_id>/review', methods=['POST'])
def add_review(prop_id):
    """
    Добавить отзыв об объекте (сохраняется в MongoDB).
    POST /properties/<prop_id>/review
    Входные (form): author, rating (1-5), text
    Выходные: redirect на /properties/<prop_id>
    """
    d = request.form
    mongo = get_mongo_db()
    mongo.reviews.insert_one({
        'property_id': prop_id,
        'author':      d['author'],
        'rating':      int(d['rating']),
        'text':        d['text'],
        'created_at':  datetime.utcnow().isoformat()
    })
    return redirect(url_for('properties.property_detail', prop_id=prop_id))


# ── GET /properties/analytics ────────────────────────────────────────────────
@properties_bp.route('/analytics')
def analytics():
    """
    Аналитика: средняя цена по типам и агентам (GROUP BY + JOIN).
    GET /properties/analytics
    Выходные: аналитические данные по типам и агентам
    """
    cur = pg_cursor()
    cur.execute("""
        SELECT   p.type,
                 COUNT(*)                 AS cnt,
                 ROUND(AVG(p.price), 0)   AS avg_price,
                 MIN(p.price)             AS min_price,
                 MAX(p.price)             AS max_price
        FROM     properties p
        GROUP BY p.type
        ORDER BY avg_price DESC
    """)
    by_type = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT   a.full_name,
                 COUNT(p.id)              AS properties_count,
                 COUNT(d.id)              AS deals_count,
                 ROUND(SUM(d.final_price), 0) AS total_revenue
        FROM     agents a
        LEFT JOIN properties p ON p.agent_id = a.id
        LEFT JOIN deals d      ON d.agent_id  = a.id
        GROUP BY a.id, a.full_name
        ORDER BY deals_count DESC
    """)
    by_agent = [dict(r) for r in cur.fetchall()]

    # Аналитика отзывов из MongoDB (агрегация)
    mongo = get_mongo_db()
    pipeline = [
        {'$group': {
            '_id': '$property_id',
            'avg_rating': {'$avg': '$rating'},
            'review_count': {'$sum': 1}
        }},
        {'$sort': {'avg_rating': -1}},
        {'$limit': 5}
    ]
    top_reviewed = list(mongo.reviews.aggregate(pipeline))

    return render_template('analytics.html',
                           by_type=by_type,
                           by_agent=by_agent,
                           top_reviewed=top_reviewed)
