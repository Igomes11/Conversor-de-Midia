import os
import smtplib
from email.message import EmailMessage

SMTP_HOST = os.getenv("SMTP_HOST", "mailpit")
SMTP_PORT = int(os.getenv("SMTP_PORT", 1025))

def enviar_email_notificacao(email_destino: str, job_id: str, download_url: str):
    msg = EmailMessage()
    msg['Subject'] = f"Seu arquivo foi convertido com sucesso! [Job: {job_id[:8]}]"
    msg['From'] = "no-reply@conversor.com"
    msg['To'] = email_destino

    conteudo = f"""
    Olá!

    Sua solicitação de conversão foi concluída.
    
    Acesse e baixe seu arquivo convertido no link abaixo:
    {download_url}

    Atenciosamente,
    Equipe Show - Conversor de Mídia
    """
    msg.set_content(conteudo)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.send_message(msg)