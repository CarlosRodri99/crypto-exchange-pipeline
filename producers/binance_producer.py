#!/usr/bin/env python3
"""
Productor de datos de Binance WebSocket
Monitoriza BTC/USDT y ETH/USDT en tiempo real
"""

import json
import os
import time
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import websocket
import threading

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración desde variables de entorno
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto-ticks')
PAIRS = os.getenv('PAIRS', 'BTCUSDT,ETHUSDT').split(',')

# Estadísticas
stats = {
    'messages_sent': 0,
    'errors': 0,
    'reconnections': 0
}


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
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            
            logger.info("✅ Conectado a Kafka exitosamente")
            return producer
            
        except Exception as e:
            logger.error(f"❌ Error conectando a Kafka: {e}")
            if attempt < max_retries - 1:
                logger.info(f"⏳ Reintentando en {retry_delay} segundos...")
                time.sleep(retry_delay)
            else:
                logger.error("❌ No se pudo conectar a Kafka después de múltiples intentos")
                raise


class BinanceWebSocketClient:
    """Cliente WebSocket para Binance con reconexión automática"""
    
    def __init__(self, pairs, producer):
        self.pairs = [pair.lower() for pair in pairs]
        self.producer = producer
        self.ws = None
        self.should_reconnect = True
        self.reconnect_delay = 5
        
    def build_stream_url(self):
        """Construye la URL del WebSocket con múltiples streams"""
        streams = []
        for pair in self.pairs:
            streams.append(f"{pair}@miniTicker")
            streams.append(f"{pair}@kline_1m")
        
        combined_streams = '/'.join(streams)
        url = f"wss://stream.binance.com:9443/stream?streams={combined_streams}"
        
        logger.info(f"📡 URL WebSocket: {url}")
        return url
    
    def on_message(self, ws, message):
        """Callback cuando se recibe un mensaje"""
        try:
            data = json.loads(message)
            
            if 'data' not in data:
                return
            
            event_data = data['data']
            event_type = event_data.get('e', '')
            
            # Procesar miniTicker
            if event_type == '24hrMiniTicker':
                processed_data = {
                    'source': 'binance',
                    'type': 'ticker',
                    'symbol': event_data.get('s', ''),
                    'timestamp': datetime.fromtimestamp(event_data.get('E', 0) / 1000).isoformat(),
                    'price': float(event_data.get('c', 0)),
                    'open_price': float(event_data.get('o', 0)),
                    'high_price': float(event_data.get('h', 0)),
                    'low_price': float(event_data.get('l', 0)),
                    'volume': float(event_data.get('v', 0)),
                    'quote_volume': float(event_data.get('q', 0)),
                    'price_change_percent': float(event_data.get('P', 0))
                }
            
            # Procesar kline (velas)
            elif event_type == 'kline':
                kline = event_data.get('k', {})
                processed_data = {
                    'source': 'binance',
                    'type': 'kline',
                    'symbol': event_data.get('s', ''),
                    'timestamp': datetime.fromtimestamp(kline.get('t', 0) / 1000).isoformat(),
                    'interval': kline.get('i', '1m'),
                    'open': float(kline.get('o', 0)),
                    'high': float(kline.get('h', 0)),
                    'low': float(kline.get('l', 0)),
                    'close': float(kline.get('c', 0)),
                    'volume': float(kline.get('v', 0)),
                    'is_closed': kline.get('x', False)
                }
            else:
                return
            
            # Enviar a Kafka
            self.send_to_kafka(processed_data)
            
        except Exception as e:
            logger.error(f"❌ Error procesando mensaje: {e}")
            stats['errors'] += 1
    
    def send_to_kafka(self, data):
        """Envía datos a Kafka"""
        try:
            future = self.producer.send(KAFKA_TOPIC, value=data)
            future.get(timeout=10)
            
            stats['messages_sent'] += 1
            
            if stats['messages_sent'] % 100 == 0:
                logger.info(f"📊 Estadísticas - Enviados: {stats['messages_sent']}, Errores: {stats['errors']}, Reconexiones: {stats['reconnections']}")
            
        except KafkaError as e:
            logger.error(f"❌ Error enviando a Kafka: {e}")
            stats['errors'] += 1
    
    def on_error(self, ws, error):
        """Callback cuando hay un error"""
        logger.error(f"❌ Error WebSocket: {error}")
        stats['errors'] += 1
    
    def on_close(self, ws, close_status_code, close_msg):
        """Callback cuando se cierra la conexión"""
        logger.warning(f"⚠️  WebSocket cerrado. Código: {close_status_code}, Mensaje: {close_msg}")
        
        if self.should_reconnect:
            logger.info(f"🔄 Reconectando en {self.reconnect_delay} segundos...")
            time.sleep(self.reconnect_delay)
            self.connect()
    
    def on_open(self, ws):
        """Callback cuando se abre la conexión"""
        logger.info("✅ WebSocket conectado exitosamente")
        logger.info(f"📈 Monitorizando pares: {', '.join([p.upper() for p in self.pairs])}")
    
    def connect(self):
        """Conecta al WebSocket"""
        try:
            stats['reconnections'] += 1
            
            url = self.build_stream_url()
            
            self.ws = websocket.WebSocketApp(
                url,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close
            )
            
            # Ejecutar en un thread separado
            wst = threading.Thread(target=self.ws.run_forever)
            wst.daemon = True
            wst.start()
            
        except Exception as e:
            logger.error(f"❌ Error conectando WebSocket: {e}")
            if self.should_reconnect:
                time.sleep(self.reconnect_delay)
                self.connect()
    
    def disconnect(self):
        """Desconecta el WebSocket"""
        logger.info("🛑 Cerrando conexión WebSocket...")
        self.should_reconnect = False
        if self.ws:
            self.ws.close()


def main():
    """Función principal"""
    logger.info("=" * 60)
    logger.info("🚀 Iniciando Binance Producer")
    logger.info("=" * 60)
    
    # Crear productor de Kafka
    producer = create_kafka_producer()
    
    # Crear cliente WebSocket
    ws_client = BinanceWebSocketClient(PAIRS, producer)
    
    try:
        # Conectar y mantener vivo
        ws_client.connect()
        
        # Mantener el programa en ejecución
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupción recibida. Cerrando...")
    finally:
        ws_client.disconnect()
        producer.close()
        logger.info("✅ Producer cerrado correctamente")


if __name__ == "__main__":
    main()
