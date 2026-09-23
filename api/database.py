# api/database.py
import os
import sqlite3
from typing import Optional, Dict

DATA_DIR = "/app/data"
os.makedirs(DATA_DIR, exist_ok=True)
DB_PATH = os.path.join(DATA_DIR, "jobs.db")

def init_db():
    """Inicializa a base de dados e cria a tabela de jobs."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                source_filename TEXT NOT NULL,
                target_format TEXT NOT NULL,
                email TEXT NOT NULL,
                status TEXT NOT NULL,
                progress INTEGER DEFAULT 0,
                output_url TEXT,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

def save_job(job_id: str, source_filename: str, target_format: str, email: str, status: str = "PENDENTE"):
    """Regista um novo job na base de dados com 0% de progresso."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO jobs (job_id, source_filename, target_format, email, status, progress)
            VALUES (?, ?, ?, ?, ?, 0)
        """, (job_id, source_filename, target_format, email, status))
        conn.commit()

def update_progress(job_id: str, progress: int, status: str = "PROCESSANDO"):
    """Atualiza a percentagem de progresso do job."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE jobs SET progress = ?, status = ? WHERE job_id = ?
        """, (progress, status, job_id))
        conn.commit()

def get_job(job_id: str) -> Optional[Dict]:
    """Retorna os dados do job pelo jobId."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None