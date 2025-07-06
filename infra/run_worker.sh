#!/bin/bash

echo "🚀 Iniciando worker conectado ao servidor local..."
prefect worker start --pool hello-pool --work-queue queue-pool