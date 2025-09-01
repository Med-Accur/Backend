from fastapi import FastAPI
from api.auth import router as auth_router
from api.widgets_select_router import router as widgets_select_router 

app = FastAPI()

app.include_router(auth_router)
app.include_router(widgets_select_router)
