FROM python:3.11-slim

COPY infra/serving/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
ENV PYTHONPATH=/app

CMD ["streamlit", "run", "serving/dashboard.py", "--server.address=0.0.0.0", "--server.port=8501"]
