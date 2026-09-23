# worker/database.py
import os
import sqlite3

DATA_DIR = "/app/data"
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "jobs.db")


def update_progress(job_id: str, progress: int, status: str = "PROCESSANDO"):
    """Atualiza o progresso diretamente no banco de dados SQLite partilhado."""
    if os.path.exists(DB_PATH):
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE jobs SET progress = ?, status = ? WHERE job_id = ?
            """, (progress, status, job_id))
            conn.commit()