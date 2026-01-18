FROM python:3.10-slim
WORKDIR /app
COPY python-app/requirements.txt .
RUN pip install --upgrade pip --no-cache-dir -r requirements.txt
COPY python-app/src/. .
#RUN mkdir -p /data /outputs
CMD ["python", "testfile.py"]
