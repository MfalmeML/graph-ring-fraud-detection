#!/bin/bash

set -e

echo "=== Starting Training Pipeline ==="

export NEO4J_URI=${NEO4J_URI:-bolt://localhost:7687}
export NEO4J_USER=${NEO4J_USER:-neo4j}
export NEO4J_PASSWORD=${NEO4J_PASSWORD:-password}
export REDIS_HOST=${REDIS_HOST:-localhost}
export REDIS_PORT=${REDIS_PORT:-6379}
export MODEL_DIR=${MODEL_DIR:-./models}

mkdir -p $MODEL_DIR

python -m src.embeddings.train_pipeline

if [ $? -eq 0 ]; then
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
    echo "Training failed."
    exit 1
fi