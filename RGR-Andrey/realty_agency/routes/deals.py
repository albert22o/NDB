from flask import Blueprint, render_template, request, redirect, url_for
from db import pg_cursor, get_redis
import json

deals_bp = Blueprint('deals', __name__)


@deals_bp.route('/')
def list_deals():
    """
    GET /deals
    Выходные: список DealEntity (JOIN properties, clients, agents)
    """
    cur = pg_cursor()
    cur.execute("""
        SELECT d.id, d.deal_date, d.final_price, d.commission,
               p.title  AS property_title, p.address, p.type,
               c.full_name AS client_name, c.phone AS client_phone,
               a.full_name AS agent_name
        FROM   deals d
        JOIN   properties p ON p.id = d.property_id
        JOIN   clients    c ON c.id = d.client_id
        JOIN   agents     a ON a.id = d.agent_id
        ORDER  BY d.deal_date DESC
    """)
    deals = [dict(r) for r in cur.fetchall()]
    return render_template('deals.html', deals=deals)


@deals_bp.route('/', methods=['POST'])
def add_deal():
    """
    POST /deals
    Входные (form): property_id, client_id, agent_id, deal_date, final_price
    Выходные: redirect на /deals
    Также меняет статус объекта на 'sold' и сбрасывает кеш.
    """
    d = request.form
    cur = pg_cursor()
    cur.execute("""
        INSERT INTO deals (property_id, client_id, agent_id, deal_date, final_price)
        VALUES (%s, %s, %s, %s, %s)
    """, (d['property_id'], d['client_id'], d['agent_id'],
          d['deal_date'], d['final_price']))
    cur.execute("UPDATE properties SET status = 'sold' WHERE id = %s", (d['property_id'],))

    # Сбрасываем кеш главной и объектов
    r = get_redis()
    r.delete('stats:main')
    for key in r.scan_iter('props:*'):
        r.delete(key)

    return redirect(url_for('deals.list_deals'))


@deals_bp.route('/new')
def new_deal_form():
    """
    GET /deals/new
    Форма для создания сделки с выборкой доступных объектов, клиентов, агентов.
    """
    cur = pg_cursor()
    cur.execute("SELECT id, title, address FROM properties WHERE status = 'available' ORDER BY title")
    available_props = cur.fetchall()
    cur.execute("SELECT id, full_name FROM clients ORDER BY full_name")
    clients = cur.fetchall()
    cur.execute("SELECT id, full_name FROM agents ORDER BY full_name")
    agents = cur.fetchall()
    return render_template('new_deal.html',
                           properties=available_props,
                           clients=clients,
                           agents=agents)
