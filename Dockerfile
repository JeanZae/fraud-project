FROM python:3.10-slim
RUN pip install --no-cache-dir uv
WORKDIR /app
COPY python-app/requirements.txt .
RUN uv pip install --no-cache --system -r requirements.txt
COPY python-app/src/. .
#RUN mkdir -p /data /outputs
CMD ["python", "testfile.py"]
