from flask import Flask

from routes.maximos import bp as maximos_bp
from routes.misc import bp as misc_bp
from routes.sync import bp as sync_bp
from routes.ventas import bp as ventas_bp
from routes.mcp import bp as mcp_bp


def create_app():
    app = Flask(__name__)
    app.register_blueprint(ventas_bp)
    app.register_blueprint(maximos_bp)
    app.register_blueprint(sync_bp)
    app.register_blueprint(misc_bp)
    app.register_blueprint(mcp_bp)
    return app


app = create_app()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)