# db_init.py
import os
from pathlib import Path

from config import Config
from models import Base, make_engine

def main():
    # Ensure instance/ exists for SQLite local dev
    if Config.SQLALCHEMY_DATABASE_URI.startswith("sqlite"):
        Path("instance").mkdir(parents=True, exist_ok=True)

    # Ensure exports/ exists for PDFs
    Path(Config.EXPORTS_DIR).mkdir(parents=True, exist_ok=True)

    engine = make_engine(Config.SQLALCHEMY_DATABASE_URI, echo=Config.SQLALCHEMY_ECHO)
    Base.metadata.create_all(engine)

    print("✅ Database initialized.")
    print(f"DB: {Config.SQLALCHEMY_DATABASE_URI}")
    print(f"Exports dir: {Config.EXPORTS_DIR}")

if __name__ == "__main__":
    main()

