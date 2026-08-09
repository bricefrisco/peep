import os
from contextlib import asynccontextmanager
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from . import db
from .repos import router as repos_router

# Explicit path, not load_dotenv()'s implicit caller-file-relative search:
# that search falls back to os.getcwd() whenever __main__ has no __file__
# (e.g. under `uvicorn --reload`, whose worker is spawned via
# multiprocessing on Windows) -- and cwd is the project root, not this
# component's own directory, so it would silently miss api/.env.
load_dotenv(Path(__file__).resolve().parent / ".env")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_pool(os.environ["DATABASE_URL"])
    yield
    db.close_pool()


app = FastAPI(title="Peep API", lifespan=lifespan)

# labeling-ui runs on Vite's dev server (a different origin than this API),
# so the browser needs an explicit CORS allowance to call it.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

app.include_router(repos_router)
