#!/bin/bash

# Requires labels from investigator review
python -c "
from src.fusion.learned_fusion import LearnedFusion
import json

with open('labels_*.json', 'r') as f:
    data = json.load(f)

# Build training data from labels
tabular_scores = {}
ring_scores = {}
labels = {}

for entry in data['labels']:
    acc = entry['account_id']
    labels[acc] = entry['label']
    # In practice, fetch tabular and ring scores from cache or API
    # This is a placeholder

fusion = LearnedFusion(epochs=50)
results = fusion.train(tabular_scores, ring_scores, labels)
print(f'Training results: {results}')
fusion.save('/app/models/fusion_model.pt')
"