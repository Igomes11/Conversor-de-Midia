# worker/converter.py
import os
import time
import subprocess
from PIL import Image
from database import update_progress


def converter_midia(job_id: str, input_path: str, output_path: str, target_format: str):
    """
    Realiza a conversão de imagens com Pillow e vídeo/áudio com FFmpeg,
    notificando a variação de progresso no SQLite.
    """
    target_format = target_format.lower().strip()
    extensoes_imagem = {"jpg", "jpeg", "png", "webp", "bmp", "gif"}

    # Progresso inicial
    update_progress(job_id, 25, "PROCESSANDO")

    # 1. Processamento de Imagem via Pillow
    if target_format in extensoes_imagem:
        time.sleep(1)  # Pausa estratégica para visualização da barra no frontend
        update_progress(job_id, 65, "PROCESSANDO")

        with Image.open(input_path) as img:
            if target_format in {"jpg", "jpeg"} and img.mode in ("RGBA", "P"):
                img = img.convert("RGB")
            img.save(output_path)

        update_progress(job_id, 90, "PROCESSANDO")
        return

    # 2. Processamento de Vídeo / Áudio via FFmpeg
    update_progress(job_id, 50, "PROCESSANDO")

    comando = ["ffmpeg", "-y", "-i", input_path, output_path]
    processo = subprocess.run(comando, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    if processo.returncode != 0:
        raise RuntimeError(f"Erro no FFmpeg: {processo.stderr}")

    update_progress(job_id, 90, "PROCESSANDO")