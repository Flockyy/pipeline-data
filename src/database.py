import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

# Load env data
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "nyc_taxi")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")

# Build pgsql url 
DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# Session maker
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class orm
Base = declarative_base()


def get_db():
    """
    Dépendance FastAPI pour obtenir une session de base de données.
    Ferme automatiquement la session après usage.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """
    Initialise les tables de la base de données en important les modèles.
    À appeler au démarrage de l'application si nécessaire.
    """
    # Importer les modèles ici pour que Base connaisse les tables
    from src import models # Assure-toi que src/models.py existe
    Base.metadata.create_all(bind=engine)