from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from api.auth import router as auth_router
from api.config import router as pre_router
app = FastAPI()

origins = [
    "http://localhost:5173", 
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,            
    allow_credentials=True,         
    allow_methods=["*"],            
    allow_headers=["*"],              
)

app.include_router(auth_router)
app.include_router(pre_router)