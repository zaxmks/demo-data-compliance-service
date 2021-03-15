from src.web.routes.healthcheck_route import healthcheck_router
from src.web.routes.pdf_routes import pdf_router

enabled_routers = [pdf_router, healthcheck_router]
