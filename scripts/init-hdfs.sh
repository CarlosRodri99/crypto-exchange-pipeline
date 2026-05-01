#!/bin/bash

echo "=========================================="
echo "🚀 Iniciando configuración de HDFS"
echo "=========================================="

# Esperar a que el NameNode esté accesible por HTTP
echo "⏳ Esperando a que NameNode esté disponible..."
max_attempts=60
attempt=0

while [ $attempt -lt $max_attempts ]; do
    if curl -f http://namenode:9870 &> /dev/null; then
        echo "✅ NameNode Web UI está respondiendo"
        break
    fi
    attempt=$((attempt + 1))
    echo "   Intento $attempt/$max_attempts..."
    sleep 2
done

if [ $attempt -eq $max_attempts ]; then
    echo "❌ ERROR: NameNode no respondió después de $max_attempts intentos"
    exit 1
fi

# Esperar un poco más para que el servicio HDFS esté completamente listo
echo "⏳ Esperando estabilización de HDFS..."
sleep 15

# Intentar crear directorios (esto fallará si está en Safe Mode, pero no pasa nada)
echo ""
echo "📁 Creando estructura de directorios en HDFS..."

directories=(
    "/crypto"
    "/crypto/raw"
    "/crypto/processed"
    "/crypto/checkpoints"
)

for dir in "${directories[@]}"; do
    hdfs dfs -mkdir -p "$dir" 2>/dev/null || true
    hdfs dfs -chmod -R 777 "$dir" 2>/dev/null || true
done

# Intentar salir del Safe Mode
echo ""
echo "📊 Intentando salir de Safe Mode..."
hdfs dfsadmin -safemode leave 2>/dev/null || true

sleep 5

# Verificar estado final
echo ""
echo "🔍 Verificando estructura creada:"
hdfs dfs -ls /crypto 2>/dev/null || echo "   (Se creará cuando Spark escriba datos)"

echo ""
echo "=========================================="
echo "✅ Inicialización de HDFS completada"
echo "=========================================="
