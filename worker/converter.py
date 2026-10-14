import os
import subprocess
from PIL import Image

def converter_midia(input_path: str, output_path: str, target_format: str, callback_progresso=None):
    # Garante conversão estrita do parâmetro para string minúscula
    fmt_destino = str(target_format).lower().strip()

    if callback_progresso:
        callback_progresso(10)

    # 1. CONVERSÃO DE IMAGENS (Pillow)
    if fmt_destino in ["jpg", "jpeg", "png", "webp", "bmp"]:
        try:
            with Image.open(input_path) as img:
                if callback_progresso: callback_progresso(50)
                
            
                if fmt_destino in ["jpg", "jpeg"] and img.mode in ("RGBA", "P"):
                    img = img.convert("RGB")
                    
                img.save(output_path)
                if callback_progresso: callback_progresso(100)
                return
        except Exception as e:
            raise RuntimeError(f"Erro ao converter imagem com Pillow: {e}")

    # 2. CONVERSÃO DE VÍDEO E ÁUDIO (FFmpeg)
    if fmt_destino in ["mp4", "avi", "mp3", "wav"]:
        try:
            cmd = ["ffmpeg", "-y", "-i", input_path]

            if fmt_destino == "mp4":
                cmd.extend(["-c:v", "libx264", "-preset", "fast", "-c:a", "aac"])
            elif fmt_destino == "avi":
                cmd.extend(["-c:v", "mpeg4", "-vtag", "xvid", "-qscale:v", "3", "-c:a", "mp3"])
            elif fmt_destino == "mp3":
                cmd.extend(["-vn", "-acodec", "libmp3lame", "-q:a", "2"])
            elif fmt_destino == "wav":
                cmd.extend(["-vn", "-acodec", "pcm_s16le"])

            cmd.append(output_path)

            if callback_progresso: callback_progresso(40)

            process = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

            if process.returncode != 0:
                raise RuntimeError(f"FFmpeg falhou com código {process.returncode}: {process.stderr[-300:]}")

            if callback_progresso: callback_progresso(100)
            return

        except Exception as e:
            raise RuntimeError(f"Erro na conversão com FFmpeg: {e}")

    raise ValueError(f"Formato de destino '{fmt_destino}' não suportado.")