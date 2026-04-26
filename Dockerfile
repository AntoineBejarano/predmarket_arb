FROM python:3.11-slim

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
