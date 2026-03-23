import psycopg2
import psycopg2.extras
from pymongo import MongoClient
import redis
import json
from flask import current_app, g


# ─────────────────────────────────────────────
#  PostgreSQL helpers
# ─────────────────────────────────────────────

def get_pg():
    if 'pg' not in g:
        cfg = current_app.config
        g.pg = psycopg2.connect(
            host=cfg['POSTGRES_HOST'],
            port=cfg['POSTGRES_PORT'],
            dbname=cfg['POSTGRES_DB'],
            user=cfg['POSTGRES_USER'],
            password=cfg['POSTGRES_PASS']
        )
        g.pg.autocommit = True
    return g.pg


def pg_cursor(conn=None):
    conn = conn or get_pg()
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


# ─────────────────────────────────────────────
#  MongoDB helpers
# ─────────────────────────────────────────────

def get_mongo_db():
    if 'mongo' not in g:
        cfg = current_app.config
        client = MongoClient(cfg['MONGO_URI'])
        g.mongo = client[cfg['MONGO_DB']]
    return g.mongo


# ─────────────────────────────────────────────
#  Redis helpers
# ─────────────────────────────────────────────

def get_redis():
    if 'redis' not in g:
        cfg = current_app.config
        g.redis = redis.Redis(
            host=cfg['REDIS_HOST'],
            port=cfg['REDIS_PORT'],
            db=cfg['REDIS_DB'],
            decode_responses=True
        )
    return g.redis


# ─────────────────────────────────────────────
#  Schema init
# ─────────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS agents (
    id          SERIAL PRIMARY KEY,
    full_name   VARCHAR(120) NOT NULL,
    phone       VARCHAR(20)  NOT NULL UNIQUE,
    email       VARCHAR(120) NOT NULL UNIQUE,
    hire_date   DATE         NOT NULL,
    commission_rate NUMERIC(4,2) NOT NULL CHECK (commission_rate >= 0 AND commission_rate <= 100)
);

CREATE TABLE IF NOT EXISTS properties (
    id            SERIAL PRIMARY KEY,
    title         VARCHAR(200) NOT NULL,
    address       VARCHAR(300) NOT NULL,
    type          VARCHAR(50)  NOT NULL,  -- apartment, house, commercial, land
    area_sqm      NUMERIC(10,2) NOT NULL CHECK (area_sqm > 0),
    price         NUMERIC(15,2) NOT NULL CHECK (price > 0),
    status        VARCHAR(30)  NOT NULL DEFAULT 'available',  -- available, reserved, sold
    agent_id      INTEGER NOT NULL REFERENCES agents(id) ON DELETE RESTRICT,
    listed_at     DATE NOT NULL DEFAULT CURRENT_DATE,
    description   TEXT
);

CREATE TABLE IF NOT EXISTS clients (
    id          SERIAL PRIMARY KEY,
    full_name   VARCHAR(120) NOT NULL,
    phone       VARCHAR(20)  NOT NULL UNIQUE,
    email       VARCHAR(120) NOT NULL UNIQUE,
    budget      NUMERIC(15,2) CHECK (budget > 0),
    registered_at DATE NOT NULL DEFAULT CURRENT_DATE
);

CREATE TABLE IF NOT EXISTS deals (
    id            SERIAL PRIMARY KEY,
    property_id   INTEGER NOT NULL REFERENCES properties(id) ON DELETE RESTRICT,
    client_id     INTEGER NOT NULL REFERENCES clients(id)   ON DELETE RESTRICT,
    agent_id      INTEGER NOT NULL REFERENCES agents(id)    ON DELETE RESTRICT,
    deal_date     DATE NOT NULL DEFAULT CURRENT_DATE,
    final_price   NUMERIC(15,2) NOT NULL CHECK (final_price > 0),
    commission    NUMERIC(15,2) GENERATED ALWAYS AS
                    (final_price * (SELECT commission_rate FROM agents WHERE id = agent_id) / 100)
                    STORED
);

CREATE INDEX IF NOT EXISTS idx_properties_status   ON properties(status);
CREATE INDEX IF NOT EXISTS idx_properties_type     ON properties(type);
CREATE INDEX IF NOT EXISTS idx_deals_deal_date     ON deals(deal_date);
"""

SEED_SQL = """
INSERT INTO agents (full_name, phone, email, hire_date, commission_rate) VALUES
  ('Иванова Мария Сергеевна',  '+7-901-111-2233', 'ivanova@realty.ru',   '2020-03-15', 3.50),
  ('Петров Алексей Игоревич',  '+7-902-222-3344', 'petrov@realty.ru',    '2019-06-01', 4.00),
  ('Сидорова Ольга Петровна',  '+7-903-333-4455', 'sidorova@realty.ru',  '2021-09-10', 3.00),
  ('Козлов Дмитрий Андреевич', '+7-904-444-5566', 'kozlov@realty.ru',    '2022-01-20', 2.50)
ON CONFLICT DO NOTHING;

INSERT INTO properties (title, address, type, area_sqm, price, status, agent_id, listed_at, description) VALUES
  ('Двухкомнатная квартира на Ленина',  'ул. Ленина, 10, кв. 45',       'apartment',  54.5,  4500000, 'available', 1, '2025-11-01', 'Светлая квартира с ремонтом, 5 этаж'),
  ('Трёхкомнатная квартира на Мира',    'ул. Мира, 22, кв. 8',          'apartment',  78.0,  6800000, 'sold',      2, '2025-09-15', 'Просторная квартира, вид на парк'),
  ('Загородный дом в Подмосковье',      'пос. Зелёное, ул. Садовая, 5', 'house',     120.0, 12000000, 'available', 1, '2025-10-20', 'Двухэтажный дом, участок 8 соток'),
  ('Офисное помещение в центре',        'пр. Победы, 1, оф. 301',       'commercial',  90.0,  9500000, 'reserved',  3, '2026-01-10', 'Офис класса B+, отличная локация'),
  ('Однокомнатная студия',              'ул. Гагарина, 55, кв. 12',     'apartment',  32.0,  2800000, 'available', 4, '2026-02-01', 'Новый дом, чистовая отделка'),
  ('Земельный участок ИЖС',             'СНТ Рассвет, участок 14',      'land',        0.15,  1500000, 'available', 2, '2026-01-25', '15 соток, коммуникации рядом'),
  ('Коттедж бизнес-класса',             'пос. Лесной, ул. Берёзовая, 3','house',     200.0, 25000000, 'sold',      2, '2025-08-05', 'Элитный коттедж, бассейн, сауна')
ON CONFLICT DO NOTHING;

INSERT INTO clients (full_name, phone, email, budget, registered_at) VALUES
  ('Николаев Борис Витальевич',  '+7-905-100-2000', 'nikolaev@mail.ru',  5000000,  '2025-10-01'),
  ('Смирнова Анна Олеговна',     '+7-906-200-3000', 'smirnova@mail.ru',  7000000,  '2025-11-15'),
  ('Фёдоров Игорь Михайлович',   '+7-907-300-4000', 'fedorov@mail.ru',  15000000,  '2026-01-05'),
  ('Захарова Татьяна Николаевна','+7-908-400-5000', 'zaharova@mail.ru',  3000000,  '2026-02-20'),
  ('Морозов Кирилл Денисович',   '+7-909-500-6000', 'morozov@mail.ru',  30000000,  '2025-09-01')
ON CONFLICT DO NOTHING;

INSERT INTO deals (property_id, client_id, agent_id, deal_date, final_price) VALUES
  (2, 2, 2, '2025-12-10', 6600000),
  (7, 5, 2, '2025-10-20', 24500000)
ON CONFLICT DO NOTHING;
"""


def init_db():
    import psycopg2
    from config import Config
    cfg = Config()
    conn = psycopg2.connect(
        host=cfg.POSTGRES_HOST, port=cfg.POSTGRES_PORT,
        dbname=cfg.POSTGRES_DB, user=cfg.POSTGRES_USER, password=cfg.POSTGRES_PASS
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        cur.execute(SEED_SQL)
    conn.close()
    print("✅ PostgreSQL schema & seed done.")
