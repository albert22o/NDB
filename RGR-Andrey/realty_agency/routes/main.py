from flask import Blueprint, render_template, jsonify
from db import get_pg, pg_cursor, get_redis, get_mongo_db
import json

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
def index():
    """Главная страница: статистика из Redis-кеша (TTL 60 сек)."""
    r = get_redis()
    cached = r.get('stats:main')
    if cached:
        stats = json.loads(cached)
    else:
        cur = pg_cursor()
        cur.execute("""
            SELECT
                COUNT(*)                                              AS total_properties,
                COUNT(*) FILTER (WHERE status = 'available')          AS available,
                COUNT(*) FILTER (WHERE status = 'sold')               AS sold,
                COUNT(*) FILTER (WHERE status = 'reserved')           AS reserved,
                ROUND(AVG(price), 0)                                  AS avg_price
            FROM properties
        """)
        stats = dict(cur.fetchone())
        cur.execute("SELECT COUNT(*) AS total_clients FROM clients")
        stats['total_clients'] = cur.fetchone()['total_clients']
        cur.execute("SELECT COUNT(*) AS total_deals FROM deals")
        stats['total_deals'] = cur.fetchone()['total_deals']
        # Кешируем с TTL 60 секунд
        r.setex('stats:main', 60, json.dumps(stats, default=str))
    return render_template('index.html', stats=stats)


@main_bp.route('/api/stats')
def api_stats():
    r = get_redis()
    cached = r.get('stats:main')
    if cached:
        return jsonify(json.loads(cached))
    return jsonify({'error': 'no cache'}), 404
