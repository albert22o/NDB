from flask import Blueprint, render_template, request, redirect, url_for, abort
from db import pg_cursor, get_mongo_db

clients_bp = Blueprint('clients', __name__)


@clients_bp.route('/')
def list_clients():
    """
    GET /clients
    Выходные: список ClientEntity + история просмотров из MongoDB
    """
    cur = pg_cursor()
    cur.execute("""
        SELECT c.*,
               COUNT(d.id) AS deals_count
        FROM   clients c
        LEFT JOIN deals d ON d.client_id = c.id
        GROUP BY c.id
        ORDER BY c.registered_at DESC
    """)
    clients = [dict(r) for r in cur.fetchall()]
    return render_template('clients.html', clients=clients)


@clients_bp.route('/<int:client_id>')
def client_detail(client_id):
    """
    GET /clients/<client_id>
    Входные: client_id (path)
    Выходные: ClientEntity + список сделок + история просмотров (MongoDB)
    """
    cur = pg_cursor()
    cur.execute("SELECT * FROM clients WHERE id = %s", (client_id,))
    client = cur.fetchone()
    if not client:
        abort(404)

    cur.execute("""
        SELECT d.id, d.deal_date, d.final_price, d.commission,
               p.title AS property_title, p.address,
               a.full_name AS agent_name
        FROM   deals d
        JOIN   properties p ON p.id = d.property_id
        JOIN   agents a     ON a.id = d.agent_id
        WHERE  d.client_id = %s
        ORDER  BY d.deal_date DESC
    """, (client_id,))
    client_deals = [dict(r) for r in cur.fetchall()]

    # История просмотров из MongoDB
    mongo = get_mongo_db()
    view_history = list(mongo.view_history.find(
        {'client_id': client_id}, {'_id': 0}
    ).sort('viewed_at', -1).limit(20))

    return render_template('client_detail.html',
                           client=dict(client),
                           deals=client_deals,
                           view_history=view_history)


@clients_bp.route('/', methods=['POST'])
def add_client():
    """
    POST /clients
    Входные (form): full_name, phone, email, budget
    Выходные: redirect на /clients
    """
    d = request.form
    cur = pg_cursor()
    cur.execute("""
        INSERT INTO clients (full_name, phone, email, budget)
        VALUES (%s, %s, %s, %s)
    """, (d['full_name'], d['phone'], d['email'], d.get('budget') or None))
    return redirect(url_for('clients.list_clients'))


@clients_bp.route('/<int:client_id>/view', methods=['POST'])
def record_view(client_id):
    """
    POST /clients/<client_id>/view
    Записывает просмотр объекта клиентом в MongoDB.
    Входные (form): property_id
    Выходные: redirect на /clients/<client_id>
    """
    from datetime import datetime
    prop_id = int(request.form['property_id'])
    mongo = get_mongo_db()
    mongo.view_history.insert_one({
        'client_id':   client_id,
        'property_id': prop_id,
        'viewed_at':   datetime.utcnow().isoformat()
    })
    return redirect(url_for('clients.client_detail', client_id=client_id))
