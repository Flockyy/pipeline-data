from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.database import engine, Base
from src.routers.trips import router
from src.database import init_db

# Create database tables (if they don’t already exist)
Base.metadata.create_all(bind=engine)

# FastAPI application configuration
app = FastAPI(
    title="NYC Taxi Data Pipeline API",
    description="API to manage New York Yellow Taxi trips and execute data import pipelines.",
    version="1.0.0"
)

# CORS configuration (open to all for development)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # should be restricted in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def on_startup():
    init_db()

# Main routes
@app.get("/", tags=["Root"])
def root():
    return {"message": "🚕 NYC Taxi API is running"}

@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "ok"}

# Include main router
app.include_router(router, prefix="/api/v1")