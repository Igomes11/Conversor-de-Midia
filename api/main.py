import os
import uuid
import json
import asyncio
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from minio import Minio
import pika

from database import init_db, save_job, get_job

app = FastAPI(
    title="API de Ingestão de Mídia",
    description="API com suporte a SSE para transmissão de progresso em tempo real",
    version="1.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

init_db()

MINIO_INTERNAL_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password123")
BUCKET_UPLOADS = "uploads"

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_QUEUE = "fila_conversao"

minio_client = Minio(
    MINIO_INTERNAL_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def publicar_mensagem_rabbitmq(payload: dict):
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    channel.basic_publish(
        exchange="",
        routing_key=RABBITMQ_QUEUE,
        body=json.dumps(payload),
        properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent)
    )
    connection.close()

@app.post("/upload", status_code=status.HTTP_202_ACCEPTED)
async def upload_midia(
    file: UploadFile = File(...),
    target_format: str = Form(...),
    notify_email: str = Form(...)
):
    job_id = str(uuid.uuid4())
    extensao_original = file.filename.split(".")[-1].lower() if "." in file.filename else ""
    object_name = f"{job_id}_{file.filename}"

    try:
        if not minio_client.bucket_exists(BUCKET_UPLOADS):
            minio_client.make_bucket(BUCKET_UPLOADS)

        minio_client.put_object(
            bucket_name=BUCKET_UPLOADS,
            object_name=object_name,
            data=file.file,
            length=-1,
            part_size=10 * 1024 * 1024,
            content_type=file.content_type
        )

        save_job(
            job_id=job_id,
            source_filename=file.filename,
            target_format=target_format.lower().strip(),
            email=notify_email,
            status="PENDENTE"
        )

        source_url = f"http://{MINIO_INTERNAL_HOST}/{BUCKET_UPLOADS}/{object_name}"
        payload = {
            "jobId": job_id,
            "sourceUrl": source_url,
            "sourceObject": object_name,
            "sourceFormat": extensao_original,
            "targetFormat": target_format.lower().strip(),
            "notifyEmail": notify_email,
            "requestedAt": datetime.utcnow().isoformat() + "Z",
            "attempt": 1
        }

        publicar_mensagem_rabbitmq(payload)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao processar ingestão: {str(e)}"
        )

    return {
        "jobId": job_id,
        "status": "PENDENTE",
        "mensagem": "Arquivo recebido e enviado para a fila."
    }

@app.get("/jobs/{job_id}")
async def consultar_status_job(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job não encontrado.")
    return job

# --- STREAMING SSE PARA A BARRA DE PROGRESSO EM TEMPO REAL ---
@app.get("/jobs/{job_id}/events")
async def stream_progress(job_id: str):
    """Envia eventos Server-Sent Events (SSE) contendo o progresso atual do Job."""
    async def event_generator():
        while True:
            job = get_job(job_id)
            if job:
                data = json.dumps({
                    "status": job["status"],
                    "progress": job.get("progress", 0),
                    "output_url": job.get("output_url")
                })
                yield f"data: {data}\n\n"
                
                if job["status"] in ["CONCLUIDO", "ERRO"]:
                    break
            await asyncio.sleep(0.5)

    return StreamingResponse(event_generator(), media_type="text/event-stream")