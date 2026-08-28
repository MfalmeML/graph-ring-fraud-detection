#!/bin/bash

set -e

echo "=== Starting Training Pipeline ==="

export NEO4J_URI=${NEO4J_URI:-bolt://localhost:7687}
export NEO4J_USER=${NEO4J_USER:-neo4j}
export NEO4J_PASSWORD=${NEO4J_PASSWORD:-password}
export REDIS_HOST=${REDIS_HOST:-localhost}
export REDIS_PORT=${REDIS_PORT:-6379}
export MODEL_DIR=${MODEL_DIR:-./models}

mkdir -p "$MODEL_DIR"

training_result=$(python -m src.embeddings.train_pipeline)
printf '%s\n' "$training_result"

training_status=$(printf '%s' "$training_result" | python -c 'import json, sys; print(json.load(sys.stdin).get("status"))')

if [ "$training_status" = "success" ]; then
    echo "Training completed successfully."
    echo "Models saved to $MODEL_DIR"
    
    # Validate models
    python -c "
import torch
import os
model_dir = os.environ.get('MODEL_DIR', './models')
try:
    torch.load(f'{model_dir}/graphsage.pt')
    print('GraphSAGE model valid')
except Exception as e:
    print(f'GraphSAGE model validation failed: {e}')
    exit(1)
try:
    torch.load(f'{model_dir}/ring_classifier.pt')
    print('Ring classifier valid')
except Exception as e:
    print(f'Ring classifier validation failed: {e}')
    exit(1)
try:
    torch.load(f'{model_dir}/fusion_model.pt')
    print('Fusion model valid')
except Exception as e:
    print(f'Fusion model validation failed: {e}')
    exit(1)
"
else
    if [ "$training_status" = "insufficient_labels" ]; then
        echo "Training not started: at least 10 confirmed labels are required."
    else
        echo "Training failed with status: $training_status"
    fi
    exit 1
fi