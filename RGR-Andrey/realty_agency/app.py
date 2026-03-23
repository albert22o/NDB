from flask import Flask
from config import Config
from db import init_db

app = Flask(__name__)
app.config.from_object(Config)

from routes.properties import properties_bp
from routes.clients import clients_bp
from routes.agents import agents_bp
from routes.deals import deals_bp
from routes.main import main_bp

app.register_blueprint(main_bp)
app.register_blueprint(properties_bp, url_prefix='/properties')
app.register_blueprint(clients_bp, url_prefix='/clients')
app.register_blueprint(agents_bp, url_prefix='/agents')
app.register_blueprint(deals_bp, url_prefix='/deals')

if __name__ == '__main__':
    init_db()
    app.run(debug=True, port=5000)
