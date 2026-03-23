from flask import Flask, render_template, request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
from decimal import Decimal
import os

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')

# ─── PostgreSQL ───────────────────────────────────────────────────────────────
POSTGRES_URL = os.environ.get(
    'DATABASE_URL',
    'postgresql://postgres:postgres@localhost:5432/carshop'
)
app.config['SQLALCHEMY_DATABASE_URI'] = POSTGRES_URL
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)

class Car(db.Model):
    __tablename__ = 'cars'

    id          = db.Column(db.Integer, primary_key=True)
    name        = db.Column(db.String(20), nullable=False)
    brand       = db.Column(db.String(20), nullable=False)
    description = db.Column(db.Text)
    price       = db.Column(db.Numeric(12, 2), nullable=False)
    stock       = db.Column(db.Integer, nullable=False, default=0)

    def __repr__(self):
        return f'<Car {self.brand} {self.name}>'


# ─── MongoDB ──────────────────────────────────────────────────────────────────
MONGO_URL = os.environ.get('MONGO_URL', 'mongodb://localhost:27017/')
mongo_client = MongoClient(MONGO_URL)
mongo_db     = mongo_client['carshop']
comments_col = mongo_db['comments']


# ─── Helpers ──────────────────────────────────────────────────────────────────
def get_avg_rating(product_id: int) -> float | None:
    """Return average rating for a car or None if no reviews."""
    pipeline = [
        {'$match': {'product_id': product_id}},
        {'$group': {'_id': None, 'avg': {'$avg': '$rating'}, 'count': {'$sum': 1}}}
    ]
    result = list(comments_col.aggregate(pipeline))
    if not result:
        return None
    return round(result[0]['avg'], 1)


def get_ratings_map(car_ids: list[int]) -> dict[int, float | None]:
    """Return {car_id: avg_rating} for a list of cars in one DB round-trip."""
    pipeline = [
        {'$match': {'product_id': {'$in': car_ids}}},
        {'$group': {'_id': '$product_id', 'avg': {'$avg': '$rating'}}}
    ]
    return {
        doc['_id']: round(doc['avg'], 1)
        for doc in comments_col.aggregate(pipeline)
    }


# ─── Routes ───────────────────────────────────────────────────────────────────

@app.route('/')
def index():
    cars = Car.query.order_by(Car.brand, Car.name).all()
    car_ids = [c.id for c in cars]
    ratings = get_ratings_map(car_ids)
    return render_template('index.html', cars=cars, ratings=ratings)


@app.route('/cars/new', methods=['GET', 'POST'])
def car_new():
    if request.method == 'POST':
        name        = request.form.get('name', '').strip()
        brand       = request.form.get('brand', '').strip()
        description = request.form.get('description', '').strip()
        price_raw   = request.form.get('price', '').strip()
        stock_raw   = request.form.get('stock', '0').strip()

        errors = []
        if not name:
            errors.append('Название обязательно.')
        if len(name) > 20:
            errors.append('Название не должно превышать 20 символов.')
        if not brand:
            errors.append('Марка обязательна.')
        if len(brand) > 20:
            errors.append('Марка не должна превышать 20 символов.')
        try:
            price = Decimal(price_raw)
            if price <= 0:
                errors.append('Цена должна быть положительной.')
        except Exception:
            errors.append('Некорректная цена.')
            price = None
        try:
            stock = int(stock_raw)
            if stock < 0:
                errors.append('Количество на складе не может быть отрицательным.')
        except Exception:
            errors.append('Некорректное количество.')
            stock = 0

        if errors:
            for e in errors:
                flash(e, 'danger')
            return render_template('car_form.html', form_data=request.form)

        car = Car(name=name, brand=brand, description=description,
                  price=price, stock=stock)
        db.session.add(car)
        db.session.commit()
        flash(f'Автомобиль «{brand} {name}» успешно добавлен!', 'success')
        return redirect(url_for('car_detail', product_id=car.id))

    return render_template('car_form.html', form_data={})


@app.route('/cars/<int:product_id>', methods=['GET', 'POST'])
def car_detail(product_id):
    car = Car.query.get_or_404(product_id)

    if request.method == 'POST':
        author  = request.form.get('author', '').strip() or 'Аноним'
        text    = request.form.get('text', '').strip()
        rating_raw = request.form.get('rating', '')

        errors = []
        if not text:
            errors.append('Текст отзыва обязателен.')
        try:
            rating = int(rating_raw)
            if not 1 <= rating <= 5:
                raise ValueError
        except (ValueError, TypeError):
            errors.append('Оценка должна быть числом от 1 до 5.')
            rating = None

        if errors:
            for e in errors:
                flash(e, 'danger')
        else:
            comments_col.insert_one({
                'product_id': product_id,
                'author':     author,
                'text':       text,
                'rating':     rating,
                'created_at': datetime.utcnow()
            })
            flash('Ваш отзыв добавлен!', 'success')

        return redirect(url_for('car_detail', product_id=product_id))

    raw_comments = list(
        comments_col.find({'product_id': product_id}).sort('created_at', -1)
    )
    avg_rating = get_avg_rating(product_id)
    return render_template('car_detail.html', car=car,
                           comments=raw_comments, avg_rating=avg_rating)


@app.route('/search')
def search():
    query      = request.args.get('q', '').strip()
    min_rating = request.args.get('min_rating', '').strip()

    if not query and not min_rating:
        return render_template('search.html', results=None,
                               query=query, min_rating=min_rating)

    # SQL: filter by name (case-insensitive)
    cars_qs = Car.query
    if query:
        cars_qs = cars_qs.filter(Car.name.ilike(f'%{query}%'))
    cars = cars_qs.order_by(Car.brand, Car.name).all()

    # MongoDB: ratings
    car_ids = [c.id for c in cars]
    ratings = get_ratings_map(car_ids)

    # Filter by minimum rating if requested
    if min_rating:
        try:
            min_r = float(min_rating)
            cars  = [c for c in cars if (ratings.get(c.id) or 0) >= min_r]
        except ValueError:
            flash('Некорректное значение минимального рейтинга.', 'warning')

    results = [
        {'car': c, 'avg_rating': ratings.get(c.id)}
        for c in cars
    ]
    return render_template('search.html', results=results,
                           query=query, min_rating=min_rating)


# ─── Init DB ──────────────────────────────────────────────────────────────────
@app.cli.command('init-db')
def init_db():
    """Create tables and insert sample data."""
    db.create_all()
    if Car.query.count() == 0:
        samples = [
            Car(name='Camry',    brand='Toyota',  description='Надёжный семейный седан.',            price=Decimal('2500000'), stock=5),
            Car(name='X5',       brand='BMW',     description='Премиальный внедорожник.',            price=Decimal('6800000'), stock=2),
            Car(name='Octavia',  brand='Skoda',   description='Практичный и экономичный автомобиль.',price=Decimal('1900000'), stock=8),
            Car(name='Model 3',  brand='Tesla',   description='Электрический седан с автопилотом.', price=Decimal('4200000'), stock=3),
            Car(name='Solaris',  brand='Hyundai', description='Бюджетный городской седан.',          price=Decimal('1200000'), stock=12),
        ]
        db.session.add_all(samples)
        db.session.commit()
        print('✓ Тестовые автомобили добавлены.')

    if comments_col.count_documents({}) == 0:
        sample_comments = [
            {'product_id': 1, 'author': 'Иван',   'text': 'Отличная машина, езжу 3 года — ни одной поломки!', 'rating': 5, 'created_at': datetime.utcnow()},
            {'product_id': 1, 'author': 'Мария',  'text': 'Хорошее качество за свои деньги.',                  'rating': 4, 'created_at': datetime.utcnow()},
            {'product_id': 2, 'author': 'Алексей','text': 'Мечта, но дорогое обслуживание.',                   'rating': 4, 'created_at': datetime.utcnow()},
            {'product_id': 3, 'author': 'Аноним', 'text': 'Лучший выбор для города.',                          'rating': 5, 'created_at': datetime.utcnow()},
            {'product_id': 4, 'author': 'Дмитрий','text': 'Зарядка неудобна за городом.',                     'rating': 3, 'created_at': datetime.utcnow()},
            {'product_id': 5, 'author': 'Ольга',  'text': 'Бюджетно и надёжно.',                               'rating': 4, 'created_at': datetime.utcnow()},
        ]
        comments_col.insert_many(sample_comments)
        print('✓ Тестовые отзывы добавлены.')
    print('База данных инициализирована.')


if __name__ == '__main__':
    app.run(debug=True)
