from flask import Blueprint, render_template, request, redirect, url_for, abort
from db import pg_cursor

agents_bp = Blueprint('agents', __name__)


@agents_bp.route('/')
def list_agents():
    """
    GET /agents
    Выходные: список AgentEntity + статистика сделок
    """
    cur = pg_cursor()
    cur.execute("""
        SELECT a.*,
               COUNT(DISTINCT p.id)  AS properties_count,
               COUNT(d.id)           AS deals_count,
               COALESCE(SUM(d.commission), 0) AS earned
        FROM   agents a
        LEFT JOIN properties p ON p.agent_id = a.id
        LEFT JOIN deals d      ON d.agent_id  = a.id
        GROUP BY a.id
        ORDER BY earned DESC
    """)
    agents = [dict(r) for r in cur.fetchall()]
    return render_template('agents.html', agents=agents)


@agents_bp.route('/', methods=['POST'])
def add_agent():
    """
    POST /agents
    Входные (form): full_name, phone, email, hire_date, commission_rate
    Выходные: redirect на /agents
    """
    d = request.form
    cur = pg_cursor()
    cur.execute("""
        INSERT INTO agents (full_name, phone, email, hire_date, commission_rate)
        VALUES (%s, %s, %s, %s, %s)
    """, (d['full_name'], d['phone'], d['email'], d['hire_date'], d['commission_rate']))
    return redirect(url_for('agents.list_agents'))
