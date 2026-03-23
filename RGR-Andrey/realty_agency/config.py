import os

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY', 'realty-secret-2026')

    # PostgreSQL
    POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'localhost')
    POSTGRES_PORT = os.environ.get('POSTGRES_PORT', '5432')
    POSTGRES_DB   = os.environ.get('POSTGRES_DB', 'realty_db')
    POSTGRES_USER = os.environ.get('POSTGRES_USER', 'postgres')
    POSTGRES_PASS = os.environ.get('POSTGRES_PASS', 'postgres')

    # MongoDB
    MONGO_URI = os.environ.get('MONGO_URI', 'mongodb://localhost:27017/')
    MONGO_DB  = os.environ.get('MONGO_DB', 'realty_mongo')

    # Redis
    REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
    REDIS_DB   = int(os.environ.get('REDIS_DB', 0))
