import os
import json
import time
import sqlite3
import pika
from minio import Minio

from converter import converter_midia
from notifier import enviar_email_notificacao

# --- Configurações das Variáveis de Ambiente ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_QUEUE = "fila_conversao"
DLQ_QUEUE = "fila_conversao_dlq"

MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password123")

BUCKET_UPLOADS = "uploads"
BUCKET_CONVERTED = "converted"
DB_PATH = "/app/jobs.db"  # Volume compartilhado ou caminho do banco SQLite

# --- Inicialização do Cliente MinIO ---
minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def atualizar_status_job(job_id: str, status: str, output_url: str = None, error_msg: str = None):
    """Atualiza o estado do Job na base SQLite compartilhada."""
    if os.path.exists(DB_PATH):
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                UPDATE jobs 
                SET status = ?, output_url = ?, error_message = ? 
                WHERE job_id = ?
            """, (status, output_url, error_msg, job_id))
            conn.commit()

def verificar_job_concluido(job_id: str) -> bool:
    """Garante a Idempotência: verifica se a mensagem já foi processada anteriormente."""
    if os.path.exists(DB_PATH):
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT status FROM jobs WHERE job_id = ?", (job_id,))
            row = cursor.fetchone()
            if row and row[0] == "CONCLUIDO":
                return True
    return False


def processar_mensagem(ch, method, properties, body):
    data = json.loads(body)
    job_id = data["jobId"]
    source_object = data["sourceObject"]
    target_format = data["targetFormat"]
    email = data["notifyEmail"]

    print(f"[*] [Worker] Iniciando Job {job_id}...")

    # 1. Checagem de Idempotência
    if verificar_job_concluido(job_id):
        print(f"[!] Job {job_id} já foi concluído anteriormente. Ignorando...")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    # Atualiza status para PROCESSANDO
    atualizar_status_job(job_id, "PROCESSANDO")

    local_input = f"/tmp/in_{job_id}_{source_object}"
    local_output = f"/tmp/out_{job_id}.{target_format}"
    converted_object = f"{job_id}.{target_format}"

    try:
        # 2. Download do arquivo de origem no MinIO
        minio_client.fget_object(BUCKET_UPLOADS, source_object, local_input)

        # 3. Execução da conversão (FFmpeg ou Pillow)
        converter_midia(local_input, local_output, target_format)

        # 4. Upload do resultado para o bucket 'converted'
        if not minio_client.bucket_exists(BUCKET_CONVERTED):
            minio_client.make_bucket(BUCKET_CONVERTED)

        minio_client.fput_object(BUCKET_CONVERTED, converted_object, local_output)

        # 5. Criação do Link Público/URL Pré-assinada
        output_url = f"http://{MINIO_HOST}/{BUCKET_CONVERTED}/{converted_object}"

        # 6. Limpeza do arquivo de origem no bucket 'uploads' (Somente após sucesso no upload)
        minio_client.remove_object(BUCKET_UPLOADS, source_object)

        # 7. Disparo da notificação por e-mail
        enviar_email_notificacao(email, job_id, output_url)

        # 8. Atualização do BD para CONCLUIDO
        atualizar_status_job(job_id, "CONCLUIDO", output_url=output_url)

        print(f"[✓] Job {job_id} processado com sucesso!")

        # 9. Confirmação Manual de Sucesso (ACK)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        erro_str = str(e)
        print(f"[X] Erro ao processar Job {job_id}: {erro_str}")

        atualizar_status_job(job_id, "ERRO", error_msg=erro_str)

        # Em caso de falha definitiva, a mensagem não ganha ACK e vai para a NACK sem requeue
        # (podendo ser roteada para a DLQ ou tratada para análise)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    finally:
        # Limpeza de arquivos temporários do disco local do worker
        if os.path.exists(local_input):
            os.remove(local_input)
        if os.path.exists(local_output):
            os.remove(local_output)


def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()

    # Declaração da fila principal
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

    # Configura o PREFETCH COUNT para 1 (essencial para distribuição entre N workers)
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=processar_mensagem)

    print(" [*] Worker aguardando mensagens no RabbitMQ. Para sair pressione CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    main()