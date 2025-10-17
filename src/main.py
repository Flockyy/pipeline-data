from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from src.database import engine, Base
from src.routers.trips import router
from src.database import init_db


# Création des tables (si non existantes)
Base.metadata.create_all(bind=engine)

# Configuration de l’application FastAPI
app = FastAPI(
    title="NYC Taxi Data Pipeline API",
    description="API pour gérer les trajets Yellow Taxi de New York et exécuter des pipelines d’importation de données.",
    version="1.0.0"
)

# Configuration CORS (ouvrir à tous pour le développement)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # à restreindre en production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def on_startup():
    init_db()

# Routes principales
@app.get("/", tags=["Root"])
def root():
    return {"message": "🚕 NYC Taxi API is running"}

@app.get("/health", tags=["Health"])
def health_check():
    return {"status": "ok"}

# Inclusion du router principal
app.include_router(router, prefix="/api/v1")