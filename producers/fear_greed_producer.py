#!/usr/bin/env python3
"""
Productor del Fear & Greed Index
Obtiene el índice de sentimiento del mercado cripto
"""

import json
import os
import time
import logging
from datetime import datetime
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'sentiment-index')
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '3600'))  # 1 hora por defecto

# Fear & Greed API
FNG_API_URL = "https://api.alternative.me/fng/"


def create_kafka_producer():
    """Crea un productor de Kafka con reintentos"""
    max_retries = 10
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Conectando a Kafka en {KAFKA_BOOTSTRAP_SERVERS} (intento {attempt + 1}/{max_retries})")
            
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks=1,
                retries=3
            )
            
            logger.info("✅ Conectado a Kafka exitosamente")
            return producer
            
        except Exception as e:
            logger.error(f"❌ Error conectando a Kafka: {e}")
            if attempt < max_retries - 1:
                logger.info(f"⏳ Reintentando en {retry_delay} segundos...")
                time.sleep(retry_delay)
            else:
                raise


def fetch_fear_greed_index():
    """Obtiene el Fear & Greed Index"""
    try:
        params = {'limit': 10}  # Obtener histórico de 10 días
        
        response = requests.get(FNG_API_URL, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        
        if 'data' not in data or not data['data']:
            logger.warning("⚠️  No se recibieron datos del API")
            return None
        
        # Procesar el índice actual y el histórico
        current = data['data'][0]
        historical = data['data'][1:]
        
        sentiment_data = {
            'source': 'fear_greed_index',
            'timestamp': datetime.utcnow().isoformat(),
            'current': {
                'value': int(current['value']),
                'classification': current['value_classification'],
                'timestamp': datetime.fromtimestamp(int(current['timestamp'])).isoformat()
            },
            'historical': [
                {
                    'value': int(item['value']),
                    'classification': item['value_classification'],
                    'timestamp': datetime.fromtimestamp(int(item['timestamp'])).isoformat()
                }
                for item in historical
            ]
        }
        
        return sentiment_data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error obteniendo Fear & Greed Index: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Error procesando datos: {e}")
        return None


def classify_sentiment(value):
    """Clasifica el valor del índice"""
    if value <= 24:
        return "Extreme Fear 😱"
    elif value <= 49:
        return "Fear 😨"
    elif value <= 74:
        return "Greed 🤑"
    else:
        return "Extreme Greed 🚀"


def main():
    """Función principal"""
    logger.info("=" * 60)
    logger.info("🚀 Iniciando Fear & Greed Index Producer")
    logger.info(f"📊 Interval de polling: {POLL_INTERVAL} segundos ({POLL_INTERVAL/3600:.1f} horas)")
    logger.info("=" * 60)
    
    # Crear productor de Kafka
    producer = create_kafka_producer()
    
    messages_sent = 0
    errors = 0
    
    try:
        while True:
            # Obtener datos
            sentiment_data = fetch_fear_greed_index()
            
            if sentiment_data:
                try:
                    # Enviar a Kafka
                    future = producer.send(KAFKA_TOPIC, value=sentiment_data)
                    future.get(timeout=10)
                    
                    messages_sent += 1
                    
                    current = sentiment_data['current']
                    value = current['value']
                    classification = classify_sentiment(value)
                    
                    logger.info(f"✅ Mensaje enviado ({messages_sent})")
                    logger.info(f"   Índice actual: {value} - {classification}")
                    logger.info(f"   Clasificación oficial: {current['classification']}")
                    
                    # Mostrar tendencia
                    if len(sentiment_data['historical']) > 0:
                        prev_value = sentiment_data['historical'][0]['value']
                        change = value - prev_value
                        trend = "📈" if change > 0 else "📉" if change < 0 else "➡️"
                        logger.info(f"   Tendencia: {trend} ({change:+d} desde ayer)")
                    
                except KafkaError as e:
                    logger.error(f"❌ Error enviando a Kafka: {e}")
                    errors += 1
            else:
                errors += 1
            
            # Esperar antes del siguiente polling
            logger.info(f"⏳ Próximo polling en {POLL_INTERVAL} segundos...")
            time.sleep(POLL_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupción recibida. Cerrando...")
    finally:
        producer.close()
        logger.info(f"📊 Estadísticas finales - Enviados: {messages_sent}, Errores: {errors}")
        logger.info("✅ Producer cerrado correctamente")


if __name__ == "__main__":
    main()
