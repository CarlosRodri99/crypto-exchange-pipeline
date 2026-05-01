#!/usr/bin/env python3
"""
Spark Streaming Job para procesamiento de datos de criptomonedas
Consume de Kafka, procesa en tiempo real y escribe a HDFS e InfluxDB
"""

import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración desde variables de entorno
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
HDFS_URL = os.getenv('HDFS_URL', 'hdfs://namenode:9000')
INFLUXDB_URL = os.getenv('INFLUXDB_URL', 'http://influxdb:8086')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN', 'admin-token-secret-12345')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG', 'crypto-org')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', 'crypto-metrics')


def create_spark_session():
    """Crea la sesión de Spark con configuración optimizada"""
    logger.info("🚀 Creando sesión de Spark...")
    
    spark = SparkSession.builder \
        .appName("CryptoStreamingPipeline") \
        .config("spark.sql.streaming.checkpointLocation.prefix", f"{HDFS_URL}/crypto/checkpoints") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,"
                "org.apache.hadoop:hadoop-client:3.2.1") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("✅ Sesión de Spark creada exitosamente")
    return spark


def get_influxdb_client():
    """Crea cliente de InfluxDB"""
    try:
        client = InfluxDBClient(
            url=INFLUXDB_URL,
            token=INFLUXDB_TOKEN,
            org=INFLUXDB_ORG
        )
        write_api = client.write_api(write_options=SYNCHRONOUS)
        logger.info("✅ Cliente InfluxDB creado")
        return client, write_api
    except Exception as e:
        logger.error(f"❌ Error creando cliente InfluxDB: {e}")
        return None, None


def write_to_influxdb(batch_df, batch_id, write_api):
    """Escribe un batch a InfluxDB"""
    try:
        if batch_df.isEmpty():
            return
        
        rows = batch_df.collect()
        points = []
        
        for row in rows:
            try:
                # Convertir Row a dict
                row_dict = row.asDict()
                
                # Crear punto para InfluxDB
                point = Point("crypto_metrics") \
                    .tag("symbol", row_dict.get('symbol', 'UNKNOWN')) \
                    .tag("source", row_dict.get('source', 'binance'))
                
                # Agregar campos según el tipo de datos
                if 'price' in row_dict and row_dict['price'] is not None:
                    point.field("price", float(row_dict['price']))
                if 'volume' in row_dict and row_dict['volume'] is not None:
                    point.field("volume", float(row_dict['volume']))
                if 'price_change_percent' in row_dict and row_dict['price_change_percent'] is not None:
                    point.field("price_change_percent", float(row_dict['price_change_percent']))
                
                # Timestamp
                if 'timestamp' in row_dict and row_dict['timestamp'] is not None:
                    point.time(row_dict['timestamp'])
                
                points.append(point)
            except Exception as e:
                logger.error(f"❌ Error procesando fila: {e}")
                continue
        
        if points and write_api:
            write_api.write(bucket=INFLUXDB_BUCKET, record=points)
            logger.info(f"✅ Batch {batch_id}: {len(points)} puntos escritos a InfluxDB")
        
    except Exception as e:
        logger.error(f"❌ Error escribiendo batch {batch_id} a InfluxDB: {e}")


def process_crypto_ticks(spark, influx_write_api):
    """Procesa el stream de ticks de Binance"""
    logger.info("📊 Iniciando procesamiento de crypto-ticks...")
    
    # Esquema para los datos de Binance
    schema = StructType([
        StructField("source", StringType(), True),
        StructField("type", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("open_price", DoubleType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("volume", DoubleType(), True),
        StructField("quote_volume", DoubleType(), True),
        StructField("price_change_percent", DoubleType(), True)
    ])
    
    # Leer de Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "crypto-ticks") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parsear JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Filtrar solo tickers (no klines)
    tickers_df = parsed_df.filter(col("type") == "ticker")
    
    # Convertir timestamp a timestamp type
    tickers_df = tickers_df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    )
    
    # Agregar marca de agua para manejar datos tardíos
    tickers_df = tickers_df.withWatermark("timestamp", "1 minute")
    
    # Simplificar: solo agregamos fecha y mandamos datos directos sin ventanas complejas
    # Las ventanas temporales se pueden hacer en Grafana
    enriched_df = tickers_df \
        .withColumn("sma_5", lit(None).cast("double")) \
        .withColumn("sma_20", lit(None).cast("double")) \
        .withColumn("volatility", lit(None).cast("double")) \
        .withColumn("fecha", to_date("timestamp"))
    
    # Escribir a HDFS (formato Parquet particionado)
    hdfs_query = enriched_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"{HDFS_URL}/crypto/processed/tickers") \
        .option("checkpointLocation", f"{HDFS_URL}/crypto/checkpoints/tickers") \
        .partitionBy("symbol", "fecha") \
        .trigger(processingTime="30 seconds") \
        .start()
    
    # Escribir a InfluxDB para visualización en tiempo real
    influx_query = enriched_df \
        .writeStream \
        .foreachBatch(lambda batch_df, batch_id: write_to_influxdb(batch_df, batch_id, influx_write_api)) \
        .outputMode("append") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    return hdfs_query, influx_query


def process_market_data(spark, influx_write_api):
    """Procesa el stream de datos de mercado de CoinGecko"""
    logger.info("📊 Iniciando procesamiento de market-data...")
    
    # Esquema simplificado para CoinGecko
    schema = StructType([
        StructField("source", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("coins", MapType(StringType(), StructType([
            StructField("price_usd", DoubleType(), True),
            StructField("market_cap_usd", DoubleType(), True),
            StructField("volume_24h_usd", DoubleType(), True),
            StructField("price_change_24h", DoubleType(), True)
        ])), True),
        StructField("global", StructType([
            StructField("total_market_cap_usd", DoubleType(), True),
            StructField("total_volume_usd", DoubleType(), True),
            StructField("btc_dominance", DoubleType(), True),
            StructField("eth_dominance", DoubleType(), True),
            StructField("active_cryptocurrencies", IntegerType(), True)
        ]), True)
    ])
    
    # Leer de Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "market-data") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parsear JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Convertir timestamp
    parsed_df = parsed_df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    ).withColumn("fecha", to_date("timestamp"))
    
    # Escribir a HDFS
    hdfs_query = parsed_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"{HDFS_URL}/crypto/processed/market") \
        .option("checkpointLocation", f"{HDFS_URL}/crypto/checkpoints/market") \
        .partitionBy("fecha") \
        .trigger(processingTime="1 minute") \
        .start()
    
    return hdfs_query


def process_sentiment_index(spark, influx_write_api):
    """Procesa el stream del Fear & Greed Index"""
    logger.info("📊 Iniciando procesamiento de sentiment-index...")
    
    # Esquema para Fear & Greed
    schema = StructType([
        StructField("source", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("current", StructType([
            StructField("value", IntegerType(), True),
            StructField("classification", StringType(), True),
            StructField("timestamp", StringType(), True)
        ]), True)
    ])
    
    # Leer de Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "sentiment-index") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parsear JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")
    
    # Convertir timestamp
    parsed_df = parsed_df.withColumn(
        "timestamp",
        to_timestamp(col("timestamp"))
    ).withColumn("fecha", to_date("timestamp"))
    
    # Escribir a HDFS
    hdfs_query = parsed_df \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", f"{HDFS_URL}/crypto/processed/sentiment") \
        .option("checkpointLocation", f"{HDFS_URL}/crypto/checkpoints/sentiment") \
        .partitionBy("fecha") \
        .trigger(processingTime="1 minute") \
        .start()
    
    return hdfs_query


def main():
    """Función principal"""
    logger.info("=" * 80)
    logger.info("🚀 CRYPTO STREAMING PIPELINE - INICIANDO")
    logger.info("=" * 80)
    logger.info(f"📡 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"💾 HDFS: {HDFS_URL}")
    logger.info(f"📊 InfluxDB: {INFLUXDB_URL}")
    logger.info("=" * 80)
    
    # Crear sesión de Spark
    spark = create_spark_session()
    
    # Crear cliente de InfluxDB
    influx_client, influx_write_api = get_influxdb_client()
    
    try:
        # Procesar los diferentes streams
        logger.info("\n📈 Iniciando queries de streaming...")
        
        ticks_hdfs, ticks_influx = process_crypto_ticks(spark, influx_write_api)
        market_query = process_market_data(spark, influx_write_api)
        sentiment_query = process_sentiment_index(spark, influx_write_api)
        
        logger.info("✅ Todas las queries iniciadas correctamente")
        logger.info("\n⏳ Pipeline en ejecución... (Ctrl+C para detener)")
        
        # Esperar a que todas las queries terminen
        ticks_hdfs.awaitTermination()
        
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupción recibida. Deteniendo queries...")
    except Exception as e:
        logger.error(f"\n❌ Error en el pipeline: {e}", exc_info=True)
    finally:
        if influx_client:
            influx_client.close()
        spark.stop()
        logger.info("✅ Pipeline cerrado correctamente")


if __name__ == "__main__":
    main()