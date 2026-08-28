#!/bin/bash

set -euo pipefail

LABELS_FILE=${1:-labels.json}
SCORES_FILE=${2:-shadow_decisions.json}
MODEL_PATH=${3:-models/fusion_model.pt}

python - "$LABELS_FILE" "$SCORES_FILE" "$MODEL_PATH" <<'PY'
from src.fusion.learned_fusion import LearnedFusion
import json
import os
import sys

labels_path, scores_path, model_path = sys.argv[1:]

with open(labels_path, 'r') as labels_file:
    label_data = json.load(labels_file)

with open(scores_path, 'r') as scores_file:
    score_data = json.load(scores_file)

tabular_scores = {}
ring_scores = {}
labels = {}

for entry in label_data['labels']:
    acc = entry['account_id']
    labels[acc] = entry['label']

for entry in score_data:
    acc = entry.get('account_id')
    if acc in labels:
        tabular_scores[acc] = entry.get('tabular_prob', 0.5)
        ring_scores[acc] = entry.get('ring_score', 0.0)

missing_scores = set(labels) - set(tabular_scores)
if missing_scores:
    raise SystemExit(f'Missing shadow scores for {len(missing_scores)} labeled accounts')

fusion = LearnedFusion(epochs=50)
results = fusion.train(tabular_scores, ring_scores, labels)
lift = fusion.compare_with_override_rule(tabular_scores, ring_scores, labels)
print(f'Training results: {results}')
print(f'Lift results: {lift}')

os.makedirs(os.path.dirname(model_path) or '.', exist_ok=True)
fusion.save(model_path)
print(f'Fusion model saved to {model_path}')
PY