#!/bin/bash

echo "🔁 Iniciando Prefect Server local..."

# Verificar se o Docker está rodando
if ! docker info > /dev/null 2>&1; then
  echo "❌ Docker não está em execução. Por favor, inicie o Docker Desktop e tente novamente."
  exit 1
fi

cd "$(dirname "$0")"
docker-compose up -d