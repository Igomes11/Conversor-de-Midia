import os
import uuid
import sqlite3
import json
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import pika
from minio import Minio

# --- CONFIGURAÇÕES DE AMBIENTE ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_QUEUE = "fila_conversao"
RABBITMQ_DLQ = "fila_conversao_dlq"
DLX_EXCHANGE = "dlx_exchange"
DLQ_ROUTING_KEY = "dlq_key"

MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "password123")
BUCKET_UPLOADS = "uploads"

DB_PATH = "/app/data/jobs.db"

app = FastAPI(title="API de Ingestão de Mídia")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- INICIALIZAÇÃO DO BANCO E MINIO ---
def init_db():
    os.makedirs("/app/data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            progress INTEGER DEFAULT 0,
            source_url TEXT,
            output_url TEXT,
            target_format TEXT,
            notify_email TEXT,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

init_db()

minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ROOT_USER,
    secret_key=MINIO_ROOT_PASSWORD,
    secure=False
)

if not minio_client.bucket_exists(BUCKET_UPLOADS):
    minio_client.make_bucket(BUCKET_UPLOADS)

def get_rabbitmq_channel():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    
    # Declara Dead Letter Exchange e DLQ conforme exigência do RNF
    channel.exchange_declare(exchange=DLX_EXCHANGE, exchange_type='direct', durable=True)
    channel.queue_declare(queue=RABBITMQ_DLQ, durable=True)
    channel.queue_bind(exchange=DLX_EXCHANGE, queue=RABBITMQ_DLQ, routing_key=DLQ_ROUTING_KEY)

    # Declara fila principal conectada à DLQ
    channel.queue_declare(
        queue=RABBITMQ_QUEUE,
        durable=True,
        arguments={
            'x-dead-letter-exchange': DLX_EXCHANGE,
            'x-dead-letter-routing-key': DLQ_ROUTING_KEY
        }
    )
    return connection, channel

# --- ROTAS DA API ---

@app.post("/upload", status_code=202)
async def upload_media(
    file: UploadFile = File(...),
    target_format: str = Form(...),
    notify_email: str = Form(...)
):
    job_id = str(uuid.uuid4())
    ext = file.filename.split(".")[-1].lower() if "." in file.filename else ""
    object_name = f"{job_id}.{ext}"

    # 1. Salva arquivo bruto no MinIO (Bucket 'uploads')
    try:
        minio_client.put_object(
            BUCKET_UPLOADS,
            object_name,
            file.file,
            length=-1,
            part_size=10 * 1024 * 1024,
            content_type=file.content_type
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao salvar arquivo no MinIO: {str(e)}")

    source_url = f"http://{MINIO_HOST}/{BUCKET_UPLOADS}/{object_name}"

    # 2. Persiste estado inicial no SQLite
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO jobs (job_id, status, progress, source_url, target_format, notify_email)
        VALUES (?, 'PENDENTE', 0, ?, ?, ?)
    """, (job_id, source_url, target_format, notify_email))
    conn.commit()
    conn.close()

    # 3. Prepara o Payload em conformidade estrita com o Contrato do PDF
    payload = {
        "jobId": job_id,
        "sourceUrl": source_url,
        "sourceType": "media",
        "sourceFormat": ext,
        "targetFormat": target_format,
        "notifyEmail": notify_email,
        "requestedAt": datetime.utcnow().isoformat() + "Z",
        "attempt": 1
    }

    # 4. Publica no RabbitMQ
    try:
        connection, channel = get_rabbitmq_channel()
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(payload),
            properties=pika.BasicProperties(
                delivery_mode=2  # Torna a mensagem persistente
            )
        )
        connection.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao publicar mensagem na fila: {str(e)}")

    # Retorno imediato HTTP 202 Accepted (RF02)
    return {"jobId": job_id, "status": "PENDENTE", "message": "Pedido de conversão aceito com sucesso."}


@app.get("/jobs/{job_id}")
def get_job_status(job_id: str):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT status, progress, output_url, error_message FROM jobs WHERE job_id = ?", (job_id,))
    row = cursor.fetchone()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="Job não encontrado.")

    return {
        "jobId": job_id,
        "status": row[0],
        "progress": row[1],
        "output_url": row[2],
        "error_message": row[3]
    }


@app.get("/jobs/{job_id}/events")
def stream_job_events(job_id: str):
    def event_generator():
        import time
        last_progress = -1
        last_status = ""

        while True:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT status, progress, output_url, error_message FROM jobs WHERE job_id = ?", (job_id,))
            row = cursor.fetchone()
            conn.close()

            if row:
                status, progress, output_url, error_message = row[0], row[1], row[2], row[3]

                if progress != last_progress or status != last_status:
                    last_progress = progress
                    last_status = status
                    data = json.dumps({
                        "jobId": job_id,
                        "status": status,
                        "progress": progress,
                        "output_url": output_url,
                        "error_message": error_message
                    })
                    yield f"data: {data}\n\n"

                if status in ["CONCLUIDO", "ERRO"]:
                    break

            time.sleep(1)

    return StreamingResponse(event_generator(), media_type="text/event-stream")