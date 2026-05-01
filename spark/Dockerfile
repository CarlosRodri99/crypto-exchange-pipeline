FROM apache/spark-py:v3.3.0

USER root

# Instalar dependencias de Python
RUN pip install --no-cache-dir \
    influxdb-client==1.36.1 \
    kafka-python==2.0.2

# Crear directorio de trabajo
WORKDIR /app

# Copiar el job de Spark
COPY crypto_streaming.py /app/crypto_streaming.py

# Comando para ejecutar el job
CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0", \
     "--master", "local[*]", \
     "--driver-memory", "2g", \
     "--executor-memory", "2g", \
     "/app/crypto_streaming.py"]
