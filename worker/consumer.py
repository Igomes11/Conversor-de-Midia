# worker/consumer.py
import os
import json
import time
import sqlite3
import pika

from converter import converter_midia
from notifier import enviar_email_notificacao
from minio_client import inicializar_buckets_e_politicas, get_minio_client
from database import update_progress

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_QUEUE = "fila_conversao"

MINIO_PUBLIC_HOST = os.getenv("MINIO_PUBLIC_HOST", "localhost:9000")

BUCKET_UPLOADS = "uploads"
BUCKET_CONVERTED = "converted"
DB_PATH = "/app/data/jobs.db"

minio_client = get_minio_client()


def atualizar_status_final(job_id: str, status: str, output_url: str = None, error_msg: str = None):
    if os.path.exists(DB_PATH):
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            progress_val = 100 if status == "CONCLUIDO" else 0
            cursor.execute("""
                UPDATE jobs 
                SET status = ?, progress = ?, output_url = ?, error_message = ? 
                WHERE job_id = ?
            """, (status, progress_val, output_url, error_msg, job_id))
            conn.commit()


def verificar_job_concluido(job_id: str) -> bool:
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

    print(f"[*] [Worker] A iniciar processamento do Job {job_id}...")

    if verificar_job_concluido(job_id):
        print(f"[!] Job {job_id} já foi concluído anteriormente. A ignorar...")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    update_progress(job_id, 10, "PROCESSANDO")

    local_input = f"/tmp/in_{job_id}_{source_object}"
    local_output = f"/tmp/out_{job_id}.{target_format}"
    converted_object = f"{job_id}.{target_format}"

    try:
        # 1. Download do ficheiro de origem
        minio_client.fget_object(BUCKET_UPLOADS, source_object, local_input)

        # 2. Conversão da mídia
        converter_midia(job_id, local_input, local_output, target_format)

        # 3. Upload do ficheiro convertido
        minio_client.fput_object(BUCKET_CONVERTED, converted_object, local_output)

        # 4. Geração do link público
        output_url = f"http://{MINIO_PUBLIC_HOST}/{BUCKET_CONVERTED}/{converted_object}"

        # 5. Remoção do ficheiro de origem
        minio_client.remove_object(BUCKET_UPLOADS, source_object)

        # 6. Notificação por e-mail
        enviar_email_notificacao(email, job_id, output_url)

        # 7. Atualização final para CONCLUIDO (100%)
        atualizar_status_final(job_id, "CONCLUIDO", output_url=output_url)

        print(f"[✓] Job {job_id} processado com sucesso!")
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        erro_str = str(e)
        print(f"[X] Erro ao processar o Job {job_id}: {erro_str}")
        atualizar_status_final(job_id, "ERRO", error_msg=erro_str)
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    finally:
        if os.path.exists(local_input):
            os.remove(local_input)
        if os.path.exists(local_output):
            os.remove(local_output)


def main():
    connection = None

    while True:
        try:
            print(f"[*] A tentar ligar ao RabbitMQ...")
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            print("[✓] Ligado ao RabbitMQ com sucesso!")
            break
        except pika.exceptions.AMQPConnectionError:
            print("[!] O RabbitMQ ainda não está pronto. A aguardar 5 segundos...")
            time.sleep(5)

    try:
        inicializar_buckets_e_politicas()
    except Exception as e:
        print(f"[!] AVISO: Não foi possível aplicar as políticas do MinIO: {e}")

    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=processar_mensagem)

    print(" [*] Worker a aguardar mensagens na fila do RabbitMQ. Prima CTRL+C para sair.")

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("[!] Interrupção manual do worker.")
        channel.stop_consuming()
        connection.close()


if __name__ == "__main__":
    main()