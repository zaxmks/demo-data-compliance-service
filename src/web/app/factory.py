from fastapi import FastAPI

from src.web.routes import enabled_routers


def create_app():
    app = FastAPI()

    for router in enabled_routers:
        app.include_router(router)

    return app
