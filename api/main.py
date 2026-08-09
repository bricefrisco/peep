import os
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI

from . import db
from .repos import router as repos_router

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_pool(os.environ["DATABASE_URL"])
    yield
    db.close_pool()


app = FastAPI(title="Peep API", lifespan=lifespan)
app.include_router(repos_router)
