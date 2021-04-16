from fastapi import FastAPI

from src.core.db.db_init import db_init
from src.web.routes import enabled_routers


def create_app():
    app = FastAPI()

    db_init()

    for router in enabled_routers:
        app.include_router(router)

    return app
