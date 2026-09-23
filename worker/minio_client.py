# worker/minio_client.py
import os
import json
from minio import Minio

MINIO_INTERNAL_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "password123")


def get_minio_client() -> Minio:
    """Retorna uma instância configurada do cliente MinIO."""
    return Minio(
        MINIO_INTERNAL_HOST,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )


def inicializar_buckets_e_politicas():
    """
    Garante a existência dos buckets 'uploads' e 'converted', e aplica
    a política de leitura pública no bucket 'converted'.
    """
    client = get_minio_client()
    bucket_uploads = "uploads"
    bucket_converted = "converted"

    # 1. Garantir que o bucket 'uploads' existe (Privado)
    if not client.bucket_exists(bucket_uploads):
        client.make_bucket(bucket_uploads)
        print(f"[✓] Bucket '{bucket_uploads}' criado com sucesso.")

    # 2. Garantir que o bucket 'converted' existe
    if not client.bucket_exists(bucket_converted):
        client.make_bucket(bucket_converted)
        print(f"[✓] Bucket '{bucket_converted}' criado com sucesso.")

    # 3. Definir a política de leitura pública para o bucket 'converted'
    policy_public_read = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": ["*"]},
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket_converted}/*"]
            }
        ]
    }

    # Aplica a política JSON ao bucket 'converted'
    client.set_bucket_policy(bucket_converted, json.dumps(policy_public_read))
    print(f"[✓] Política de leitura pública aplicada com sucesso ao bucket '{bucket_converted}'.")


if __name__ == "__main__":
    inicializar_buckets_e_politicas()