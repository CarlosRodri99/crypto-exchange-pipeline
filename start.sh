#!/bin/bash

# Script de inicio automatizado para el pipeline de criptomonedas
# Autor: [Tu nombre]
# Fecha: $(date +%Y-%m-%d)

set -e

echo "=========================================="
echo "🚀 CRYPTO EXCHANGE PIPELINE"
echo "   Iniciando sistema completo..."
echo "=========================================="
echo ""

# Colores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Función para logging
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[⚠]${NC} $1"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
}

# Verificar que Docker está instalado
log_info "Verificando Docker..."
if ! command -v docker &> /dev/null; then
    log_error "Docker no está instalado. Por favor instálalo primero."
    exit 1
fi
log_success "Docker encontrado: $(docker --version)"

# Verificar Docker Compose v2
log_info "Verificando Docker Compose..."
if ! docker compose version &> /dev/null; then
    log_error "Docker Compose v2 no está disponible."
    log_warning "Usa 'docker compose' en lugar de 'docker-compose'"
    exit 1
fi
log_success "Docker Compose encontrado: $(docker compose version)"

# Limpiar contenedores anteriores si existen
log_info "Limpiando contenedores anteriores..."
docker compose down 2>/dev/null || true
log_success "Limpieza completada"

# Crear directorios necesarios
log_info "Creando estructura de directorios..."
mkdir -p monitoring/grafana/provisioning/{datasources,dashboards}
chmod +x scripts/init-hdfs.sh 2>/dev/null || true
log_success "Directorios creados"

# Construir imágenes
log_info "Construyendo imágenes Docker..."
docker compose build --no-cache
log_success "Imágenes construidas"

echo ""
echo "=========================================="
log_info "FASE 1: Levantando infraestructura base"
echo "=========================================="

# Levantar Zookeeper y Kafka
log_info "Iniciando Zookeeper y Kafka..."
docker compose up -d zookeeper kafka
log_info "Esperando a que Kafka esté listo (esto puede tardar ~40 segundos)..."

# Esperar a que Kafka esté healthy
max_wait=60
elapsed=0
while [ $elapsed -lt $max_wait ]; do
    if docker compose ps kafka | grep -q "healthy"; then
        log_success "Kafka está listo y operativo"
        break
    fi
    echo -n "."
    sleep 5
    elapsed=$((elapsed + 5))
done
echo ""

if [ $elapsed -ge $max_wait ]; then
    log_error "Kafka no estuvo listo en $max_wait segundos"
    log_info "Mostrando logs de Kafka:"
    docker compose logs kafka | tail -20
    exit 1
fi

# Levantar HDFS
log_info "Iniciando HDFS NameNode y DataNode..."
docker compose up -d namenode datanode
log_info "Esperando a que NameNode esté listo (~30 segundos)..."

sleep 30

# Verificar estado de HDFS
if docker compose ps namenode | grep -q "healthy"; then
    log_success "HDFS NameNode está listo"
else
    log_warning "NameNode aún está iniciando, esperando más tiempo..."
    sleep 20
fi

# Inicializar HDFS (sale del safe mode y crea directorios)
log_info "Inicializando HDFS y creando estructura de directorios..."
docker compose up hdfs-init

# Esperar a que termine la inicialización
sleep 10

# Verificar que HDFS salió del safe mode
log_info "Verificando estado de HDFS..."
if docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; then
    log_success "HDFS está operativo (Safe Mode OFF)"
else
    log_warning "HDFS podría estar en Safe Mode. Intentando forzar salida..."
    docker exec namenode hdfs dfsadmin -safemode leave 2>/dev/null || true
    sleep 5
fi

# Mostrar estructura de HDFS creada
log_info "Estructura de directorios en HDFS:"
docker exec namenode hdfs dfs -ls -R /crypto 2>/dev/null || log_warning "No se pudo listar HDFS (podría estar aún inicializando)"

echo ""
echo "=========================================="
log_info "FASE 2: Levantando servicios de monitoreo"
echo "=========================================="

log_info "Iniciando InfluxDB, Prometheus, Grafana..."
docker compose up -d influxdb prometheus grafana node-exporter cadvisor

log_info "Esperando a que InfluxDB esté listo..."
sleep 20

if docker compose ps influxdb | grep -q "healthy"; then
    log_success "InfluxDB está listo"
else
    log_warning "InfluxDB aún está iniciando"
fi

echo ""
echo "=========================================="
log_info "FASE 3: Levantando productores"
echo "=========================================="

log_info "Iniciando productores de datos..."
docker compose up -d binance-producer coingecko-producer fear-greed-producer

log_success "Productores iniciados"

echo ""
echo "=========================================="
log_info "FASE 4: Levantando Spark Streaming"
echo "=========================================="

log_info "Iniciando Spark Streaming (esto puede tardar unos segundos)..."
docker compose up -d spark-streaming

log_success "Spark Streaming iniciado"

echo ""
echo "=========================================="
log_success "✅ SISTEMA COMPLETAMENTE LEVANTADO"
echo "=========================================="
echo ""

# Mostrar estado de los servicios
log_info "Estado de los servicios:"
docker compose ps

echo ""
echo "=========================================="
log_info "📊 URLs de Acceso"
echo "=========================================="
echo ""
echo "  🎯 Grafana:        http://localhost:3000"
echo "     Usuario: admin | Contraseña: admin"
echo ""
echo "  📈 Prometheus:     http://localhost:9090"
echo ""
echo "  💾 InfluxDB:       http://localhost:8086"
echo "     Usuario: admin | Contraseña: admin-password"
echo ""
echo "  📁 HDFS NameNode:  http://localhost:9870"
echo ""
echo "  ⚡ Spark UI:       http://localhost:4040"
echo ""
echo "  🐳 cAdvisor:       http://localhost:8080"
echo ""
echo "=========================================="
log_info "📝 Comandos Útiles"
echo "=========================================="
echo ""
echo "  Ver logs en tiempo real:"
echo "    docker compose logs -f"
echo ""
echo "  Ver logs de un servicio específico:"
echo "    docker compose logs -f spark-streaming"
echo "    docker compose logs -f binance-producer"
echo ""
echo "  Verificar Kafka:"
echo "    docker exec -it kafka kafka-topics --list --bootstrap-server localhost:9092"
echo ""
echo "  Verificar HDFS:"
echo "    docker exec -it namenode hdfs dfs -ls -R /crypto"
echo ""
echo "  Detener todo:"
echo "    docker compose down"
echo ""
echo "  Limpiar todo (¡cuidado, borra datos!):"
echo "    docker compose down -v"
echo ""
echo "=========================================="

# Verificar que los productores están enviando datos
echo ""
log_info "Verificando que los productores están enviando datos..."
sleep 5

log_info "Últimos logs de binance-producer:"
docker compose logs --tail=5 binance-producer

echo ""
log_info "Sistema listo. Presiona Ctrl+C para ver los logs en tiempo real"
echo ""

# Opción de ver logs
read -p "¿Quieres ver los logs en tiempo real? (s/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Ss]$ ]]; then
    docker compose logs -f
fi
