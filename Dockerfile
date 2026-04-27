FROM python:3.11-slim

# OpenMP: LightGBM carga libgomp.so.1 al importar/joblib.load (sin esto falla en slim)
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml .

RUN uv pip install --system --no-cache \
    pandas pyarrow tqdm requests python-dotenv \
    lightgbm "scikit-learn>=1.6.1,<1.7" "scipy>=1.13" numpy rich joblib \
    fastapi uvicorn websocket-client aiohttp "websockets>=12" \
    eth-account "py-order-utils>=0.3.2"

COPY . .

RUN mkdir -p data/raw data/zips data/features \
             models/saved reports logs static

EXPOSE 8080

CMD ["python", "scripts/api.py"]
