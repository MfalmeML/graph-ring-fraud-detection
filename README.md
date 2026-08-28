# Graph-Based Ring Fraud Detection

Real-time detection of coordinated fraud rings using graph construction, community detection, and graph neural networks — built to catch collusive fraud that transaction-level models can't see.

## The problem

Traditional fraud models score one transaction at a time:

```
customer | amount | merchant | device | time
```

That's blind to coordinated fraud. A ring of accounts sharing the same device, IP, or merchant can each generate transactions that look perfectly normal in isolation — while the *relationships between them* are highly abnormal.

```
          Device A
         /   |   \
       C1    C2   C3
       |     |     |
      IP1   IP1   IP2
       \     |     /
        Merchant X
```

This project represents the transaction ecosystem as a graph — accounts, devices, IPs, merchants, cards — and uses graph features, embeddings, and GNNs to surface these rings, fused into a production real-time decision engine.

## How it fits with a tabular fraud system

This is designed as an **addition to**, not a replacement for, a transaction-level fraud model. The graph layer produces one more signal — a per-account `ring_score` — that feeds the same risk engine and decision policy as your existing model.

```
Tabular fraud_probability (per transaction)
              │
              ├──────────────┐
              ▼              ▼
      Graph ring_score   FUSION LAYER  →  combined_risk_score
                                              │
                                              ▼
                                        RISK ENGINE
                                              │
                                              ▼
                                      DECISION POLICY
                                              │
                                              ▼
                          APPROVE / CHALLENGE / DECLINE / INVESTIGATE
```

## Architecture

```
PAYMENT EVENT STREAM (Kafka)
        │
        ▼
┌───────────────────┐
│  ENTITY EXTRACTION │   account, device, IP, merchant, card
└─────────┬──────────┘
          │
          ▼
┌───────────────────┐
│   GRAPH BUILDER     │   upsert nodes + edges
└─────────┬──────────┘
          │
   ┌──────┴───────┐
   ▼              ▼
GRAPH STORE   GRAPH SNAPSHOT
(Neo4j, live)  (offline, for training)
   │              │
   ▼              ▼
GRAPH FEATURES  GRAPH EMBEDDINGS
(degree, cycles,  (Node2Vec / GraphSAGE)
 shared-entity     │
 counts)           │
   └──────┬────────┘
          ▼
   RING / GNN MODEL
          │
          ▼
  community_risk_score
          │
          ▼
   FUSION LAYER   ← combines with tabular fraud_probability
          │
          ▼
     RISK ENGINE → DECISION POLICY
```

**Two latency paths, by design:**

| Path | Budget | Purpose |
|---|---|---|
| Authorization-time (hot) | ~40 ms | Precomputed `ring_score` lookup from a KV store + fusion + decision |
| Graph update (warm) | seconds–minutes | Kafka consumer upserts the graph, recomputes structural features |
| Batch analytics (cold) | scheduled | Full graph snapshot, community detection, GNN retraining |

Live graph traversal never sits on the authorization hot path — scores are precomputed and cached, refreshed asynchronously.

## Entity & relationship schema

**Nodes:** `Account`, `Device`, `IP`, `Merchant`, `Card`
**Edges:** `USED` (Account↔Device, Account↔IP), `OWNS` (Account↔Card), `TRANSACTED_WITH` (Account↔Merchant), `SEEN_AT` (Device↔IP)

```json
{
  "node_type": "Account",
  "account_id": "cust_456",
  "created_at": "2026-01-10T00:00:00Z",
  "account_age_days": 42,
  "kyc_verified": true
}
```

See [`docs/specification.md`](docs/specification.md) for the full schema, label definitions, feature set, decision policy, and evaluation metrics.

## Build sequence

The project is built in stages, each shipping value before the next is attempted:

1. **Entity resolution + graph construction** from the existing Kafka stream — no ML yet
2. **Structural features** (device/IP account counts, triangle counts, clustering coefficient) added as columns to an existing tabular model — cheap validation that the graph is worth building
3. **Unsupervised community detection** (Louvain / connected components) surfaces candidate rings to investigators — label-free, and generates labels for step 5
4. **Precomputed `ring_score`** in a KV store + override rule in the decision policy — ships value before any GNN exists
5. **GraphSAGE embeddings + supervised ring classifier**, once investigator-confirmed labels accumulate
6. **Learned fusion layer** replacing the manual override rule, validated against the business-impact equation

## Shadow-mode rollout

Run the shadow evaluator against a 1% sample of the transaction stream. It publishes decisions to `shadow_decisions` without changing production decisions:

```bash
docker compose -f deployment/docker-compose.yml up -d shadow-mode
python scripts/run_shadow_analysis.py --bootstrap localhost:9092 --limit 10000 --output shadow_decisions.json
```

After investigators review enough candidates, export the confirmed labels and train the fusion model using the collected shadow scores:

```bash
curl http://localhost:8002/rings/labels/export > labels.json
./scripts/train_fusion.sh labels.json shadow_decisions.json models/fusion_model.pt
```

Review the reported validation metrics and `lift_auc` / `lift_auprc` against the override rule. Only after the lift is acceptable, restart shadow mode so it loads the model from `/app/models/fusion_model.pt`:

```bash
docker compose -f deployment/docker-compose.yml restart shadow-mode
```

## Tech stack

- **Streaming:** Kafka
- **Graph store:** Neo4j
- **Graph analysis (offline/prototyping):** NetworkX
- **Embeddings / GNN:** PyTorch Geometric, Node2Vec, GraphSAGE
- **Serving:** FastAPI, Docker
- **Fraud baseline:** XGBoost / LightGBM (tabular)

## Repository structure

```
graph-ring-fraud-detection/
│
├── ingestion/
│   ├── transaction_consumer/
│   └── graph_builder/
│
├── graph/
│   ├── schema/
│   ├── snapshotting/
│   └── entity_resolution/
│
├── features/
│   ├── structural/
│   ├── temporal_graph/
│   └── embeddings/
│
├── models/
│   ├── community_detection/
│   ├── graphsage/
│   └── fusion/
│
├── serving/
│   ├── ring-score-api/
│   └── feature-cache/
│
├── decision/
│   ├── rules/
│   └── risk-engine/
│
├── monitoring/
│   ├── graph-drift/
│   └── ring-precision/
│
├── docs/
│   └── specification.md
│
└── tests/
```

## Evaluation

- **Ring detection:** precision/recall of flagged clusters against investigator-confirmed rings; time-to-detection
- **Classification:** PR-AUC / ROC-AUC for `ring_score`, using temporal (not random) train/validation/test splits
- **Fusion lift:** does `combined_risk_score` outperform the tabular-only score on held-out, time-split fraud loss?
- **Operational:** graph query latency (p50/p95/p99), cache hit rate, graph store availability
- **Drift:** structural feature distribution shift over time — fraud rings restructure to evade detection, the graph-native version of concept drift

## Status

🚧 Early stage — see [`docs/specification.md`](docs/specification.md) for the full technical spec this is being built against.

## License

MIT