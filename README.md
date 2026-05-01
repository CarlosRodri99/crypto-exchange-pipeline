# 🚀 Crypto Exchange Pipeline - Proyecto Final

Pipeline de análisis de criptomonedas en tiempo real usando Apache Kafka, Spark Streaming, HDFS y Grafana.

## 📋 Tabla de Contenidos

- [Arquitectura](#arquitectura)
- [Requisitos](#requisitos)
- [Instalación](#instalación)
- [Uso](#uso)
- [Componentes](#componentes)
- [Troubleshooting](#troubleshooting)
- [Monitorización](#monitorización)

## 🏗️ Arquitectura

```
┌─────────────────┐
│ Binance WS      │───┐
│ CoinGecko API   │───┼──► Kafka ──► Spark Streaming ──┬──► HDFS (Parquet)
│ Fear&Greed API  │───┘                                 └──► InfluxDB ──► Grafana
└─────────────────┘
```

### Componentes Principales

1. **Fuentes de Datos**:
   - Binance WebSocket (BTC/USDT, ETH/USDT)
   - CoinGecko REST API (Market Cap, Dominancia)
   - Fear & Greed Index API (Sentimiento del mercado)

2. **Ingesta**: Apache Kafka con 3 topics
   - `crypto-ticks`: Ticks de precios en tiempo real
   - `market-data`: Datos de mercado globales
   - `sentiment-index`: Índice de sentimiento

3. **Procesamiento**: Spark Streaming
   - Cálculo de medias móviles (SMA 5min, 20min)
   - Detección de volatilidad
   - Agregaciones temporales

4. **Almacenamiento**:
   - HDFS: Histórico en formato Parquet particionado
   - InfluxDB: Métricas en tiempo real

5. **Visualización**:
   - Grafana: Dashboards en vivo
   - Prometheus: Monitorización de infraestructura

## 📦 Requisitos

- Docker Engine 20.10+
- Docker Compose v2+
- 8GB RAM mínimo (16GB recomendado)
- 20GB espacio en disco

## 🚀 Instalación

### 1. Clonar el repositorio

```bash
git clone <tu-repo>
cd crypto-exchange-pipeline
```

### 2. Verificar Docker

```bash
docker --version
docker compose version
```

**IMPORTANTE**: Asegúrate de usar `docker compose` (v2) y NO `docker-compose` (v1).

### 3. Levantar el cluster

```bash
# Opción 1: Usando el script automatizado (RECOMENDADO)
chmod +x start.sh
./start.sh

# Opción 2: Manual
docker compose up -d
```

### 4. Verificar el estado

```bash
docker compose ps
```

Todos los servicios deberían mostrar estado `healthy` o `running`.

## 📊 Uso

### Acceder a los Servicios

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | - |
| InfluxDB | http://localhost:8086 | admin / admin-password |
| HDFS NameNode UI | http://localhost:9870 | - |
| Spark UI | http://localhost:4040 | - |
| cAdvisor | http://localhost:8080 | - |

### Verificar Kafka

```bash
# Listar topics
docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092

# Ver mensajes en tiempo real
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic crypto-ticks \
  --from-beginning
```

### Verificar HDFS

```bash
# Listar archivos en HDFS
docker exec -it namenode hdfs dfs -ls /crypto/processed

# Ver estructura completa
docker exec -it namenode hdfs dfs -ls -R /crypto
```

### Ver Logs

```bash
# Todos los servicios
docker compose logs -f

# Servicio específico
docker compose logs -f spark-streaming
docker compose logs -f binance-producer
```

## 🔧 Componentes

### Productores

#### Binance Producer
- **Fuente**: Binance WebSocket
- **Pares**: BTC/USDT, ETH/USDT
- **Frecuencia**: Tiempo real (~1 msg/segundo por par)
- **Streams**: `@miniTicker`, `@kline_1m`

#### CoinGecko Producer
- **Fuente**: CoinGecko REST API
- **Frecuencia**: Cada 60 segundos
- **Datos**: Market cap, volumen, dominancia BTC/ETH

#### Fear & Greed Producer
- **Fuente**: Alternative.me API
- **Frecuencia**: Cada 1 hora
- **Datos**: Índice de sentimiento (0-100)

### Spark Streaming

**Indicadores Calculados**:
- Media móvil simple (SMA) de 5 minutos
- Media móvil simple (SMA) de 20 minutos
- Volatilidad (desviación estándar)
- Variación porcentual

**Salidas**:
1. HDFS (histórico): `/crypto/processed/{tickers,market,sentiment}`
2. InfluxDB (tiempo real): bucket `crypto-metrics`

### HDFS

**Estructura de directorios**:
```
/crypto
├── processed/
│   ├── tickers/      # Particionado por symbol, fecha
│   ├── market/       # Particionado por fecha
│   └── sentiment/    # Particionado por fecha
├── checkpoints/      # Checkpoints de Spark Streaming
└── raw/             # Datos crudos (opcional)
```

**Formato**: Parquet con compresión Snappy

## 🐛 Troubleshooting

### Problema: HDFS en Safe Mode

**Síntoma**: 
```
SafeModeException: Cannot create /crypto/checkpoints. Name node is in safe mode.
```

**Solución Automática**:
El servicio `hdfs-init` se encarga automáticamente. Verificar:

```bash
docker compose logs hdfs-init
```

**Solución Manual**:
```bash
docker exec -it namenode hdfs dfsadmin -safemode leave
docker compose restart spark-streaming
```

### Problema: Kafka no está listo

**Síntoma**: Productores fallan al conectar

**Solución**:
```bash
# Verificar healthcheck de Kafka
docker compose ps kafka

# Si no está healthy, esperar ~30 segundos
# El healthcheck tiene start_period de 40s
```

### Problema: Spark no puede escribir a HDFS

**Verificar permisos**:
```bash
docker exec -it namenode hdfs dfs -chmod -R 777 /crypto
```

**Recrear directorios**:
```bash
docker exec -it namenode bash /scripts/init-hdfs.sh
```

### Problema: InfluxDB no acepta datos

**Verificar token**:
```bash
docker exec -it influxdb influx auth list
```

**Recrear bucket**:
```bash
docker compose down influxdb
docker volume rm crypto-exchange-pipeline_influxdb_data
docker compose up -d influxdb
```

## 📈 Monitorización

### Dashboards de Grafana

1. **Crypto Prices (Real-time)**
   - Precio actual de BTC/ETH
   - Medias móviles
   - Volatilidad

2. **Market Overview**
   - Market cap total
   - Dominancia BTC
   - Volumen 24h

3. **Sentiment Analysis**
   - Fear & Greed Index
   - Tendencia histórica

4. **Infrastructure**
   - CPU/Memoria por contenedor
   - Uso de disco HDFS
   - Lag de Kafka

### Métricas de Prometheus

**Infraestructura**:
- Node Exporter: métricas del host
- cAdvisor: métricas de contenedores

**Queries útiles**:
```promql
# CPU por contenedor
rate(container_cpu_usage_seconds_total[1m])

# Memoria por contenedor
container_memory_usage_bytes

# Uso de disco
node_filesystem_avail_bytes
```

## 🛠️ Comandos Útiles

### Reiniciar todo el pipeline

```bash
docker compose down
docker compose up -d
```

### Limpiar volúmenes (¡CUIDADO! Borra todos los datos)

```bash
docker compose down -v
```

### Escalar productores

```bash
docker compose up -d --scale binance-producer=2
```

### Backup de datos HDFS

```bash
docker exec namenode hdfs dfs -get /crypto/processed ./backup/
```

## 📝 Decisiones de Diseño

### Topics de Kafka

**Diseño elegido**: 3 topics separados por tipo de fuente

**Justificación**:
- ✅ Separación lógica de responsabilidades
- ✅ Diferentes frecuencias de actualización
- ✅ Facilita el escalado independiente
- ❌ Más complejidad en la gestión

**Alternativas consideradas**:
- Topic único: más simple pero pierde granularidad
- Topics por par: escalabilidad pero overhead de gestión

### Particionado HDFS

**Diseño elegido**: `symbol` + `fecha` para tickers, solo `fecha` para market/sentiment

**Justificación**:
- ✅ Queries filtradas por par son muy eficientes
- ✅ Pruning de particiones por fecha
- ✅ Balance entre granularidad y número de archivos
- ❌ Puede generar muchos archivos pequeños con baja actividad

### Ventanas Temporales de Spark

| Indicador | Ventana | Deslizamiento | Justificación |
|-----------|---------|---------------|---------------|
| SMA 5min | 5 min | 30 seg | Detecta movimientos rápidos |
| SMA 20min | 20 min | 30 seg | Tendencia a medio plazo |
| Volatilidad | 5 min | 30 seg | Alertas tempranas de pump&dump |

## 📚 Referencias

- [Binance WebSocket API](https://binance-docs.github.io/apidocs/spot/en/#websocket-market-streams)
- [CoinGecko API](https://www.coingecko.com/en/api/documentation)
- [Fear & Greed Index](https://alternative.me/crypto/fear-and-greed-index/)
- [Apache Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [InfluxDB Flux](https://docs.influxdata.com/influxdb/v2.0/query-data/get-started/)

## 👥 Autor

[Carlos Rodriguez] - Proyecto Final Big Data

## 📄 Licencia

Este proyecto es académico y no tiene licencia para uso comercial.
