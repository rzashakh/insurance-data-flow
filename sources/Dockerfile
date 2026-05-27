FROM python:3.11-slim
RUN pip install --no-cache-dir psycopg2-binary
WORKDIR /app
COPY source_simulator.py .
CMD ["python", "source_simulator.py"]
