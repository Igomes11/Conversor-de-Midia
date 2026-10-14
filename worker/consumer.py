import os
import json
import sqlite3
import time
import pika
from minio import Minio

from converter import converter_midia
from notifier import enviar_email_notificacao

# --- CONFIGURAÇÕES DE AMBIENTE ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_QUEUE = "fila_conversao"
RABBITMQ_DLQ = "fila_conversao_dlq"
DLX_EXCHANGE = "dlx_exchange"
DLQ_ROUTING_KEY = "dlq_key"

MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_PUBLIC_HOST = os.getenv("MINIO_PUBLIC_HOST", "localhost:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "password123")

BUCKET_UPLOADS = "uploads"
BUCKET_CONVERTED = "converted"
DB_PATH = "/app/data/jobs.db"
MAX_ATTEMPTS = 3  # Máximo de tentativas antes de mover para DLQ

# --- CLIENTE MINIO ---
minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ROOT_USER,
    secret_key=MINIO_ROOT_PASSWORD,
    secure=False
)

# Garante a existência do bucket converted público
if not minio_client.bucket_exists(BUCKET_CONVERTED):
    minio_client.make_bucket(BUCKET_CONVERTED)
    policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": ["*"]},
            "Action": ["s3:GetObject"],
            "Resource": [f"arn:aws:s3:::{BUCKET_CONVERTED}/*"]
        }]
    }
    minio_client.set_bucket_policy(BUCKET_CONVERTED, json.dumps(policy))


def atualizar_job(job_id, status=None, progress=None, output_url=None, error_message=None):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    updates = []
    params = []

    if status:
        updates.append("status = ?")
        params.append(status)
    if progress is not None:
        updates.append("progress = ?")
        params.append(progress)
    if output_url:
        updates.append("output_url = ?")
        params.append(output_url)
    if error_message is not None:
        updates.append("error_message = ?")
        params.append(error_message)

    params.append(job_id)
    query = f"UPDATE jobs SET {', '.join(updates)} WHERE job_id = ?"
    cursor.execute(query, params)
    conn.commit()
    conn.close()


def processar_mensagem(ch, method, properties, body):
    try:
        data = json.loads(body)
        job_id = str(data["jobId"])
        source_url = str(data["sourceUrl"])
    
        target_format = str(data["targetFormat"]).lower().strip()
        notify_email = str(data["notifyEmail"])
        attempt = data.get("attempt", 1)

        print(f"[*] [Worker] Iniciando Job {job_id} -> Formato Alvo: '{target_format}' (Tentativa {attempt})...")

        # 1. Checagem de Idempotência
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM jobs WHERE job_id = ?", (job_id,))
        row = cursor.fetchone()
        conn.close()

        if row and row[0] == "CONCLUIDO":
            print(f"[✓] [Worker] Job {job_id} já concluído anteriormente. Invocando ACK sem reprocessar.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        # 2. Atualiza estado para PROCESSANDO
        atualizar_job(job_id, status="PROCESSANDO", progress=10)

        # 3. Download do arquivo fonte do MinIO bucket 'uploads'
        source_object_name = source_url.split("/")[-1]
        local_input_path = f"/tmp/{source_object_name}"
        local_output_path = f"/tmp/{job_id}.{target_format}"

        minio_client.fget_object(BUCKET_UPLOADS, source_object_name, local_input_path)
        atualizar_job(job_id, progress=30)

        # 4. Executa Conversão
        print(f"[*] [Worker] Convertendo {source_object_name} para {target_format}...")
        
        def callback_progresso(p):
            # Mapeia progresso de conversão para escala 30% -> 80%
            progresso_real = 30 + int(p * 0.5)
            atualizar_job(job_id, progress=progresso_real)

        converter_midia(local_input_path, local_output_path, target_format, callback_progresso)
        atualizar_job(job_id, progress=85)

        # 5. Upload do resultado para bucket 'converted'
        converted_object_name = f"{job_id}.{target_format}"
        minio_client.fput_object(
            BUCKET_CONVERTED,
            converted_object_name,
            local_output_path
        )

        output_url = f"http://{MINIO_PUBLIC_HOST}/{BUCKET_CONVERTED}/{converted_object_name}"
        atualizar_job(job_id, progress=95)

        # 6. Limpeza do arquivo original do bucket 'uploads' (RF05)
        try:
            minio_client.remove_object(BUCKET_UPLOADS, source_object_name)
            print(f"[✓] Arquivo de origem {source_object_name} removido do bucket 'uploads'.")
        except Exception as e:
            print(f"[!] Aviso: Não foi possível remover o arquivo de origem: {e}")

        # 7. Limpeza dos arquivos temporários locais
        if os.path.exists(local_input_path): os.remove(local_input_path)
        if os.path.exists(local_output_path): os.remove(local_output_path)

        # 8. Notificação por E-mail (RF06)
        enviar_email_notificacao(notify_email, job_id, output_url)

        # 9. Conclusão com Sucesso
        atualizar_job(job_id, status="CONCLUIDO", progress=100, output_url=output_url)
        print(f"[✓] [Worker] Job {job_id} finalizado com sucesso!")

        # Confirmação manual (ACK)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        err_msg = str(e)
        print(f"[X] [Worker] Erro no processamento do Job {job_id if 'job_id' in locals() else 'Desconhecido'}: {err_msg}")

        # Tratamento de Retentativas e envio para Dead Letter Queue (DLQ)
        attempt = data.get("attempt", 1) if 'data' in locals() else 1
        
        if attempt < MAX_ATTEMPTS and 'job_id' in locals():
            print(f"[!] Re-enfileirando mensagem para nova tentativa ({attempt + 1}/{MAX_ATTEMPTS})...")
            data["attempt"] = attempt + 1
            atualizar_job(job_id, status="PENDENTE", error_message=f"Tentativa {attempt} falhou: {err_msg}")
            
            # Re-publica com incremento de tentativa
            ch.basic_publish(
                exchange='',
                routing_key=RABBITMQ_QUEUE,
                body=json.dumps(data),
                properties=pika.BasicProperties(delivery_mode=2)
            )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        else:
            # Excedeu tentativas: Marca como ERRO e envia para DLQ descartando com requeue=False
            if 'job_id' in locals():
                atualizar_job(job_id, status="ERRO", error_message=f"Falha definitiva após {attempt} tentativas: {err_msg}")
            
            print("[X] Limite de tentativas atingido. Redirecionando mensagem para a Dead Letter Queue (DLQ)...")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)


def iniciar_consumer():
    while True:
        try:
            connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
            channel = connection.channel()

            # Declaração das filas e DLQ no RabbitMQ
            channel.exchange_declare(exchange=DLX_EXCHANGE, exchange_type='direct', durable=True)
            channel.queue_declare(queue=RABBITMQ_DLQ, durable=True)
            channel.queue_bind(exchange=DLX_EXCHANGE, queue=RABBITMQ_DLQ, routing_key=DLQ_ROUTING_KEY)

            channel.queue_declare(
                queue=RABBITMQ_QUEUE,
                durable=True,
                arguments={
                    'x-dead-letter-exchange': DLX_EXCHANGE,
                    'x-dead-letter-routing-key': DLQ_ROUTING_KEY
                }
            )

            # Prefetch = 1 para distribuição justa entre os Workers concorrentes
            channel.basic_qos(prefetch_count=1)
            channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=processar_mensagem)

            print("[*] Worker pronto e aguardando por mensagens. Para sair pressione CTRL+C")
            channel.start_consuming()

        except pika.exceptions.AMQPConnectionError:
            print("[!] Conexão com o RabbitMQ perdeu o sinal. Tentando reconectar em 5 segundos...")
            time.sleep(5)
        except Exception as e:
            print(f"[X] Erro inesperado no consumo do Worker: {e}")
            time.sleep(5)

if __name__ == "__main__":
    iniciar_consumer()