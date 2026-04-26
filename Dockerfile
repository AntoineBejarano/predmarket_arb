FROM python:3.11-slim

# OpenMP: LightGBM carga libgomp.so.1 al importar/joblib.load (sin esto falla en slim)
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml .

RUN uv pip install --system --no-cache \
    pandas pyarrow tqdm requests python-dotenv \
    lightgbm scikit-learn scipy numpy rich joblib \
    fastapi uvicorn

COPY . .

RUN mkdir -p data/raw data/zips data/features \
             models/saved reports logs static

EXPOSE 8080

CMD ["python", "scripts/api.py"]
