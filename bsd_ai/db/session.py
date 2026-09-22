from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from ..config import settings
from .models import Base

engine = create_engine(settings.database_url, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)


def init_db() -> None:
    """Crée les tables si absentes. Pour un vrai projet en évolution, remplacer
    par Alembic dès que le schéma bouge en prod avec des données déjà collectées."""
    Base.metadata.create_all(engine)


def get_session() -> Session:
    return SessionLocal()
