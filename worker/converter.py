import os
import subprocess
from PIL import Image

def converter_midia(input_path: str, output_path: str, target_format: str):
    """
    Identifica se a mídia é imagem, áudio ou vídeo e aplica a conversão adequada.
    """
    target_format = target_format.lower().strip()
    extensoes_imagem = {"jpg", "jpeg", "png", "webp", "bmp", "gif"}

    # 1. Processamento de Imagem via Pillow
    if target_format in extensoes_imagem:
        with Image.open(input_path) as img:
            # Converte RGBA para RGB se o destino for JPG/JPEG
            if target_format in {"jpg", "jpeg"} and img.mode in ("RGBA", "P"):
                img = img.convert("RGB")
            img.save(output_path)
        return

    # 2. Processamento de Vídeo e Áudio via FFmpeg
    # Executa o FFmpeg direto pelo sistema operacional
    comando = [
        "ffmpeg",
        "-y",               # Sobrescreve o arquivo de saída se existir
        "-i", input_path,   # Arquivo de entrada
        output_path         # Arquivo de saída
    ]

    processo = subprocess.run(
        comando,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if processo.returncode != 0:
        raise RuntimeError(f"Erro no FFmpeg: {processo.stderr}")