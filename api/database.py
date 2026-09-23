import sqlite3
from typing import Optional, Dict

DB_PATH = "jobs.db"

def init_db():
    """Inicializa a tabela de jobs no SQLite caso não exista."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id TEXT PRIMARY KEY,
                source_filename TEXT NOT NULL,
                target_format TEXT NOT NULL,
                email TEXT NOT NULL,
                status TEXT NOT NULL,
                output_url TEXT,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

def save_job(job_id: str, source_filename: str, target_format: str, email: str, status: str = "PENDENTE"):
    """Insere um novo job no banco com o status inicial PENDENTE."""
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO jobs (job_id, source_filename, target_format, email, status)
            VALUES (?, ?, ?, ?, ?)
        """, (job_id, source_filename, target_format, email, status))
        conn.commit()

def get_job(job_id: str) -> Optional[Dict]:
    """Busca o status e os metadados de um job pelo jobId."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,))
        row = cursor.fetchone()
        if row:
            return dict(row)
        return None