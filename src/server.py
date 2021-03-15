import uvicorn

from src.web.app.factory import create_app

main_app = create_app()

if __name__ in "__main__":
    uvicorn.run(main_app, host="0.0.0.0", port=8080)
