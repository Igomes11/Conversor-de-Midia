FROM python:3.11-slim-bookworm

# Define variáveis para evitar interatividade no apt e otimizar chamadas
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Instala depências do sistema com tratamento de falhas e retentativas do APT
RUN set -eux; \
    apt-get update -y && \
    apt-get install -y --no-install-recommends \
        ffmpeg \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copia e instala pacotes Python com retentativas em caso de instabilidade de rede
COPY requirements.txt .
RUN pip install --timeout=60 --retries=5 -r requirements.txt

# Copia o código-fonte da aplicação
COPY . .

# Comando de execução
CMD ["python", "consumer.py"]