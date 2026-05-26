FROM python:3.11-slim
WORKDIR /app
RUN pip install kafka-python redis requests
COPY consumer.py .
CMD ["python", "consumer.py"]
