FROM python:3.12-slim

# OpenMP: LightGBM carga libgomp.so.1 al importar/joblib.load (sin esto falla en slim)
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

COPY pyproject.toml .

# Mismas versiones que [project].dependencies en pyproject.toml (runtime API/arb/ML)
RUN uv pip install --system --no-cache \
    "pandas>=2.0.0" "pyarrow>=14.0.0" "tqdm>=4.65.0" "requests>=2.31.0" "python-dotenv>=1.0.0" \
    "lightgbm>=4.0.0" "scikit-learn>=1.6.1,<1.7" "scipy>=1.13.0" "aiohttp>=3.9.0" "websockets>=12.0" \
    "eth-account>=0.13.0" "py-order-utils>=0.3.2" "numpy>=1.24.0" "rich>=13.0.0" "joblib>=1.3.0" \
    "fastapi>=0.110.0" "uvicorn>=0.27.0" "websocket-client>=1.6.0"

COPY . .

RUN mkdir -p data/raw data/zips data/features \
             models/saved reports logs static

EXPOSE 8080

CMD ["python", "scripts/api.py"]
