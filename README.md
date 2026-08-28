# Graph-Based Ring Fraud Detection System

Real-time graph-based fraud detection for coordinated fraud rings that may be invisible to transaction-level models. The graph layer is designed as an additional signal alongside an existing tabular fraud model.

## Architecture

```text
Kafka transactions
        |
        v
Graph Builder ----------> Neo4j graph store
        |                         |
        |                         v
        +----------------> Structural and temporal features
                                  |
                                  v
                         Ring Score API <---- Redis feature cache
                                  |
                                  v
                  Fusion layer and decision policy
                                  |
                 APPROVE / CHALLENGE / DECLINE / INVESTIGATE

Community detection (batch) --> Investigator API
                                      |
                         Confirm/reject labels
                                      |
                           Fusion model training

Shadow mode consumes transaction events and publishes comparison decisions to
the shadow_decisions Kafka topic without changing production decisions.
```

## Components

- **Event ingestion**: Parses transaction events and publishes graph entities and relationships through the Kafka consumer.
- **Graph builder**: Upserts accounts, devices, IPs, merchants, cards, and relationships into Neo4j.
- **Graph store**: Neo4j 5.14 with the Graph Data Science plugin.
- **Feature computation**: Calculates structural and temporal graph signals.
- **Feature cache**: Redis stores precomputed or recently computed scores for low-latency lookups.
- **Ring Score API**: FastAPI service exposing account ring scores.
- **Community detection**: Finds candidate rings using connected components and Louvain-style community analysis.
- **Investigator API**: Provides a pending-ring review queue, ring details, confirm/reject actions, and label export.
- **GraphSAGE and embeddings**: Provides the embedding and supervised-classifier path for graph-native features.
- **Fusion layer**: Combines tabular fraud probability with ring score using a manual override or learned PyTorch model.
- **Shadow mode**: Samples production-like Kafka traffic, compares shadow and production decisions, and writes audit records to Kafka.
- **Fallback path**: Keeps tabular-only scoring available when graph services are unavailable.

## Repository Layout

```text
deployment/
  docker-compose.yml       Local service topology
  Dockerfile.base          Shared dependency image
  Dockerfile.api           Ring Score API image
  Dockerfile.batch         Community batch image
  Dockerfile.builder       Kafka graph-builder image
  Dockerfile.investigator  Investigator API image
  Dockerfile.shadow        Shadow-mode image
  shadow_mode.py           Shadow runner entry point
scripts/
  run_shadow_analysis.py   Read and summarize shadow decisions
  train_fusion.sh          Train and save the fusion model
src/
  ingestion/                Kafka event parsing and consumption
  graph_builder/            Graph node, edge, and schema models
  features/                 Structural and temporal features
  community/                Detection, batch jobs, and investigator interface
  serving/                  APIs, caching, scoring, and fallback
  fusion/                   Learned fusion model
  embeddings/               Graph embedding and classifier training
  config/                   Decision policy
tests/                      Unit and integration tests
```

## Quick Start

### Prerequisites

- Docker Desktop with Docker Compose
- GNU Make 4.4+ (Git Bash, WSL, or a Unix shell on Windows)
- Python 3.11+ and a project virtual environment
- Network access to Docker Hub and Python package indexes

The Docker images use Python 3.11. The local virtual environment may use Python 3.12; install a compatible Torch version there if needed.

### Installation

```bash
git clone <repository>
cd graph-ring-fraud-detection
python -m venv .venv
source .venv/bin/activate             # Git Bash or Linux/macOS
make install
```

On Windows PowerShell, activate with `.venv\\Scripts\\Activate.ps1`, or run the Makefile with the installed Make executable. The Makefile uses `.venv/Scripts/python.exe` for its Python commands.

### Start the stack

```bash
make docker-up
```

`docker-up` first builds `graph-ring-fraud-base:latest`, which installs the shared dependencies once, then starts the Compose services. The base image uses CPU-only Torch to avoid CUDA runtime downloads.

Services:

- Neo4j browser: [http://localhost:7474](http://localhost:7474/) and Bolt at `localhost:7687`
- Redis: `localhost:6379`
- Kafka: `localhost:9092`
- Ring Score API: `http://localhost:8000`
- Ring Score secondary listener: `localhost:8001`
- Investigator API: `http://localhost:8002`

Default credentials are `neo4j` / `password` for local development only.

### Run tests

```bash
make test
```

The target starts Neo4j, Redis, Kafka, and Zookeeper with Compose health checks, waits for them to become healthy, and then runs all tests with the project virtual environment. It does not tear those services down afterward.

To stop the stack:

```bash
make docker-down
```

## Make Commands

| Command | Purpose |
|---|---|
| `make help` | List common targets |
| `make install` | Install `requirements.txt` into `.venv` |
| `make test` | Start test infrastructure and run `tests/` |
| `make docker-up` | Build the shared image and start all services |
| `make docker-down` | Stop and remove Compose services |
| `make shadow` | Run the shadow-mode worker in the foreground |
| `make train` | Run fusion training from the default input files |

## Configuration

Compose supplies these service variables:

| Variable | Default/example | Used by |
|---|---|---|
| `NEO4J_URI` | `bolt://neo4j:7687` | Graph services |
| `NEO4J_USER` | `neo4j` | Graph services |
| `NEO4J_PASSWORD` | `password` | Graph services |
| `REDIS_HOST` | `redis` | API, batch, shadow, fusion |
| `REDIS_PORT` | `6379` | API, batch, shadow, fusion |
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Ingestion and shadow |
| `KAFKA_TOPIC` | `transactions` | Graph builder |
| `SHADOW_INPUT_TOPIC` | `transactions` | Shadow mode |
| `SHADOW_OUTPUT_TOPIC` | `shadow_decisions` | Shadow mode configuration |
| `SHADOW_SAMPLE_RATE` | `0.01` | Shadow mode |
| `FUSION_MODEL_PATH` | `/app/models/fusion_model.pt` | Shadow mode |

The local Compose file mounts `./models` into `/app/models` for the ring-score and shadow services. Create that directory before starting the stack if Docker does not create it automatically.

## APIs

### Ring score lookup

```http
GET /ring-score/{account_id}
```

Example response:

```json
{
  "account_id": "acc_001",
  "ring_score": 0.85,
  "cached": true
}
```

### Investigator review

```http
GET  /rings/pending?limit=50
GET  /rings/{ring_id}
POST /rings/confirm
POST /rings/reject
GET  /rings/labels/export
```

Confirm and reject requests use:

```json
{
  "ring_id": "ring_001",
  "status": "CONFIRMED",
  "investigator_id": "analyst_001"
}
```

The label export returns `{ "labels": [...] }`, with account IDs, ring IDs, binary labels, and investigator metadata.

## Decision Policy

The decision policy supports an interpretable override and a combined score:

1. A confirmed ring with `ring_score > 0.90` and at least two confirmed members is escalated for investigation.
2. Otherwise, the combined score is calculated as:

   `combined_score = alpha * tabular_probability + (1 - alpha) * ring_score`

3. Score thresholds are:
   - Above `0.90`: `DECLINE`
   - Above `0.50` through `0.90`: `CHALLENGE`
   - At or below `0.50`: `APPROVE`

The fallback module preserves tabular-only operation when the graph score is unavailable.

## Data Model

### Nodes

- `Account`
- `Device`
- `IP`
- `Merchant`
- `Card`

### Relationships

- `Account -[USED]-> Device`
- `Account -[USED]-> IP`
- `Account -[TRANSACTED_WITH]-> Merchant`
- `Account -[OWNS]-> Card`
- `Device -[SEEN_AT]-> IP`
- `Account -[BELONGS_TO_RING]-> RingCandidate`

### Features

Structural features include device and IP account counts, merchant account diversity, triangle count, clustering coefficient, connected-component size, and shared-entity count.

Temporal features include new edges in the last hour and day and edge-formation burstiness.

Learned features include account embeddings and distance to the nearest confirmed-fraud embedding. GraphSAGE training code is under `src/embeddings/`.

## Shadow Mode and Training

Shadow mode is configured with `SHADOW_SAMPLE_RATE=0.01`. It consumes `transactions`, computes graph and fusion decisions, and publishes comparison records to `shadow_decisions` without changing production decisions.

Start it with:

```bash
make shadow
```

Collect analysis records:

```bash
python scripts/run_shadow_analysis.py \
  --bootstrap localhost:9092 \
  --topic shadow_decisions \
  --limit 10000 \
  --output shadow_decisions.json
```

After investigator review, export labels from the Investigator API:

```bash
curl http://localhost:8002/rings/labels/export > labels.json
```

Train and report validation metrics plus lift against the override rule:

```bash
./scripts/train_fusion.sh labels.json shadow_decisions.json models/fusion_model.pt
```

The training script requires a shadow score for every labeled account. It saves the model only after training succeeds. Review `val_auc`, `val_auprc`, `lift_auc`, and `lift_auprc` on held-out data before enabling the learned model. Restart shadow mode after placing the model in the mounted `models` directory:

```bash
docker compose -f deployment/docker-compose.yml restart shadow-mode
```

Do not enable the learned fusion model solely because it has a positive offline metric. Require acceptable fraud-loss lift, customer friction, investigation volume, latency, and drift behavior.

## Evaluation and Monitoring

- Ring precision and recall against investigator-confirmed rings
- Time to ring detection
- ROC-AUC and PR-AUC using time-split evaluation data
- Learned fusion lift over the manual override and tabular-only baselines
- Fraud loss prevented versus customer friction, investigation cost, and infrastructure cost
- Graph query latency at p50, p95, and p99
- Redis cache hit rate and service availability
- Ring-score and feature-distribution drift
- Investigator queue volume and label quality
- Kafka consumer lag and shadow decision mismatch rate

Live graph traversal should not sit on the authorization hot path. Use cached or precomputed scores and monitor the fallback rate.

## Operational Notes

- The Compose file includes local development credentials and must not be used unchanged for production secrets.
- Kafka advertises `localhost:9092` for local clients; container-to-container clients use `kafka:9092`.
- Docker builds can be large because of scientific Python dependencies. The shared base image prevents five repeated installs, and CPU-only Torch avoids CUDA packages.
- `Dockerfile.investigator` and the shared base image are required by the Compose configuration.
- `make test` requires the infrastructure ports to be available and does not mock external services.
- The tests include both local unit-style tests and service-dependent integration tests.

## Build Sequence

1. Parse transaction events and construct the Neo4j graph.
2. Compute structural features and validate their value with the tabular model.
3. Run community detection and send candidates to investigators.
4. Cache `ring_score` and apply the interpretable override rule.
5. Accumulate confirmed and rejected labels.
6. Train GraphSAGE/classifier components and the learned fusion layer.
7. Validate time-split lift and business impact.
8. Roll out learned fusion gradually with the tabular-only fallback available.

## License

MIT
