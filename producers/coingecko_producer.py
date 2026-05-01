#!/usr/bin/env python3
"""
Productor de datos de CoinGecko API REST
Obtiene market cap, volumen y dominancia de BTC
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
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'market-data')
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '60'))

# CoinGecko API
COINGECKO_API_BASE = "https://api.coingecko.com/api/v3"
COINS = ['bitcoin', 'ethereum']


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


def fetch_market_data():
    """Obtiene datos de mercado de CoinGecko"""
    try:
        # Datos de precios y market cap
        price_url = f"{COINGECKO_API_BASE}/simple/price"
        params = {
            'ids': ','.join(COINS),
            'vs_currencies': 'usd',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true',
            'include_24hr_change': 'true'
        }
        
        response = requests.get(price_url, params=params, timeout=10)
        response.raise_for_status()
        
        price_data = response.json()
        
        # Datos globales
        global_url = f"{COINGECKO_API_BASE}/global"
        global_response = requests.get(global_url, timeout=10)
        global_response.raise_for_status()
        
        global_data = global_response.json()['data']
        
        # Construir mensaje
        market_data = {
            'source': 'coingecko',
            'timestamp': datetime.utcnow().isoformat(),
            'coins': {},
            'global': {
                'total_market_cap_usd': global_data.get('total_market_cap', {}).get('usd', 0),
                'total_volume_usd': global_data.get('total_volume', {}).get('usd', 0),
                'btc_dominance': global_data.get('market_cap_percentage', {}).get('btc', 0),
                'eth_dominance': global_data.get('market_cap_percentage', {}).get('eth', 0),
                'active_cryptocurrencies': global_data.get('active_cryptocurrencies', 0)
            }
        }
        
        # Agregar datos por moneda
        for coin in COINS:
            if coin in price_data:
                coin_data = price_data[coin]
                market_data['coins'][coin] = {
                    'price_usd': coin_data.get('usd', 0),
                    'market_cap_usd': coin_data.get('usd_market_cap', 0),
                    'volume_24h_usd': coin_data.get('usd_24h_vol', 0),
                    'price_change_24h': coin_data.get('usd_24h_change', 0)
                }
        
        return market_data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error obteniendo datos de CoinGecko: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Error procesando datos: {e}")
        return None


def main():
    """Función principal"""
    logger.info("=" * 60)
    logger.info("🚀 Iniciando CoinGecko Producer")
    logger.info(f"📊 Interval de polling: {POLL_INTERVAL} segundos")
    logger.info("=" * 60)
    
    # Crear productor de Kafka
    producer = create_kafka_producer()
    
    messages_sent = 0
    errors = 0
    
    try:
        while True:
            # Obtener datos
            market_data = fetch_market_data()
            
            if market_data:
                try:
                    # Enviar a Kafka
                    future = producer.send(KAFKA_TOPIC, value=market_data)
                    future.get(timeout=10)
                    
                    messages_sent += 1
                    
                    logger.info(f"✅ Mensaje enviado ({messages_sent})")
                    logger.info(f"   BTC: ${market_data['coins']['bitcoin']['price_usd']:,.2f} | "
                              f"Market Cap: ${market_data['global']['total_market_cap_usd']/1e9:.2f}B | "
                              f"BTC Dominance: {market_data['global']['btc_dominance']:.2f}%")
                    
                except KafkaError as e:
                    logger.error(f"❌ Error enviando a Kafka: {e}")
                    errors += 1
            else:
                errors += 1
            
            # Esperar antes del siguiente polling
            time.sleep(POLL_INTERVAL)
            
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupción recibida. Cerrando...")
    finally:
        producer.close()
        logger.info(f"📊 Estadísticas finales - Enviados: {messages_sent}, Errores: {errors}")
        logger.info("✅ Producer cerrado correctamente")


if __name__ == "__main__":
    main()
