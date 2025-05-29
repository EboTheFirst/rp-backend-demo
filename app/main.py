from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from contextlib import asynccontextmanager
from .core.config import settings
from .core.data import load_data
from .routers import customers, merchants, upload

@asynccontextmanager
async def lifespan(app: FastAPI):
    if settings.AUTO_RELOAD and settings.csv_path.exists():
        load_data()
    yield

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(upload.router)    
app.include_router(customers.router)
app.include_router(merchants.router)
