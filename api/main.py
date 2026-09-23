import os
import uuid
import json
from datetime import datetime
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, status
from fastapi.responses import JSONResponse
from minio import Minio
import pika

from database import init_db, save_job, get_job

app = FastAPI(
    title="API de Ingestão de Mídia",
    description="Recebe arquivos, salva no MinIO e envia requisição de conversão para o RabbitMQ",
    version="1.0.0"
)

# Inicializa a tabela no banco ao carregar a aplicação
init_db()

# --- Configurações via Variáveis de Ambiente ---
MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password123")
BUCKET_UPLOADS = "uploads"

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_QUEUE = "fila_conversao"

# --- Inicialização do Cliente MinIO ---
minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def publicar_mensagem_rabbitmq(payload: dict):
    """Conecta ao RabbitMQ e publica a mensagem persistente na fila."""
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # Declara a fila durável (não se perde se o RabbitMQ reiniciar)
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    # Publica o JSON na fila de conversão
    channel.basic_publish(
        exchange="",
        routing_key=RABBITMQ_QUEUE,
        body=json.dumps(payload),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent  # Mensagem gravada em disco
        )
    )
    connection.close()


@app.post("/upload", status_code=status.HTTP_202_ACCEPTED)
async def upload_midia(
    file: UploadFile = File(...),
    target_format: str = Form(...),
    notify_email: str = Form(...)
):
    """
    Recebe o arquivo de mídia, armazena no MinIO, enfileira o trabalho no RabbitMQ
    e responde imediatamente com o jobId.
    """
    job_id = str(uuid.uuid4())
    extensa_original = file.filename.split(".")[-1].lower() if "." in file.filename else ""
    object_name = f"{job_id}_{file.filename}"

    try:
        # 1. Garantir que o bucket 'uploads' existe no MinIO
        if not minio_client.bucket_exists(BUCKET_UPLOADS):
            minio_client.make_bucket(BUCKET_UPLOADS)

        # 2. Salvar o arquivo no bucket 'uploads' do MinIO
        minio_client.put_object(
            bucket_name=BUCKET_UPLOADS,
            object_name=object_name,
            data=file.file,
            length=-1,
            part_size=10 * 1024 * 1024,  # Trabalha em partes de 10MB
            content_type=file.content_type
        )

        # 3. Registrar o Job no SQLite com status PENDENTE
        save_job(
            job_id=job_id,
            source_filename=file.filename,
            target_format=target_format.lower().strip(),
            email=notify_email,
            status="PENDENTE"
        )

        # 4. Montar o payload enxuto conforme especificação do contrato
        source_url = f"http://{MINIO_HOST}/{BUCKET_UPLOADS}/{object_name}"
        payload = {
            "jobId": job_id,
            "sourceUrl": source_url,
            "sourceObject": object_name,
            "sourceFormat": extensa_original,
            "targetFormat": target_format.lower().strip(),
            "notifyEmail": notify_email,
            "requestedAt": datetime.utcnow().isoformat() + "Z",
            "attempt": 1
        }

        # 5. Publicar a mensagem no RabbitMQ
        publicar_mensagem_rabbitmq(payload)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro ao processar a requisição de ingestão: {str(e)}"
        )

    # 6. Retorno imediato para o usuário
    return {
        "jobId": job_id,
        "status": "PENDENTE",
        "mensagem": "Arquivo recebido com sucesso e enviado para a fila de conversão."
    }


@app.get("/jobs/{job_id}")
async def consultar_status_job(job_id: str):
    """Endpoint para permitir que o Frontend consulte o status do pedido."""
    job = get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Pedido (jobId) não encontrado."
        )
    return job