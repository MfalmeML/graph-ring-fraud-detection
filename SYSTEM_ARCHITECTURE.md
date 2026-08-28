# Graph-Based Ring Fraud Detection System Architecture

## System Overview

This is a real-time graph-based fraud detection system designed to identify coordinated fraud rings that may be invisible to transaction-level models. The system operates as an additional signal layer alongside existing tabular fraud models.

## High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              TRANSACTION EVENT STREAM                                │
│                                    (Kafka)                                           │
└────────────────────────────────┬────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              EVENT INGESTION LAYER                                   │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  TransactionConsumer (kafka_consumer.py)                                     │  │
│  │  - Consumes transaction events from Kafka                                     │  │
│  │  - Entity resolution for deduplication                                        │  │
│  │  - Batches events for efficient processing                                    │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  EventParser (event_parser.py)                                                │  │
│  │  - Parses transaction events into graph nodes and edges                       │  │
│  │  - Creates: Account, Device, IP, Merchant, Card nodes                         │  │
│  │  - Creates: USED, TRANSACTED_WITH, OWNS, SEEN_AT edges                        │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────┬────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                              GRAPH BUILDER LAYER                                    │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  BatchGraphLoader (batch_loader.py)                                          │  │
│  │  - Batches node and edge operations                                          │  │
│  │  - Performs MERGE operations for idempotency                                  │  │
│  │  - Groups by node/edge type for efficiency                                    │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  EntityResolver (entity_resolution.py)                                      │  │
│  │  - Resolves duplicate entities across transactions                            │  │
│  │  - Handles fuzzy matching for entities                                        │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │  TTLManager (ttl_manager.py)                                                 │  │
│  │  - Cleans up old nodes and edges based on TTL                                 │  │
│  │  - Manages graph storage lifecycle                                             │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────┬────────────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                           GRAPH STORAGE LAYER                                        │
│  ┌──────────────────────────────────────────────────────────────────────────────┐  │
│  │                           Neo4j 5.14                                           │  │
│  │                    + Graph Data Science Plugin                                │  │
│  │  Nodes: Account, Device, IP, Merchant, Card, RingCandidate                    │  │
│  │  Edges: USED, TRANSACTED_WITH, OWNS, SEEN_AT, BELONGS_TO_RING                 │  │
│  └──────────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────┘
                                 │
                ┌────────────────┼────────────────┐
                ▼                ▼                ▼
┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐
│  FEATURE COMPUTATION │  │  COMMUNITY DETECTION │  │  EMBEDDING LAYER     │
└──────────────────────┘  └──────────────────────┘  └──────────────────────┘
```

## Detailed Component Architecture

### 1. Ingestion Layer

**Components:**
- `TransactionConsumer` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\ingestion\kafka_consumer.py" />)
- `EventParser` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\ingestion\event_parser.py" />)
- `EntityResolver` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\graph_builder\entity_resolution.py" />)

**Data Flow:**
```
Kafka Topic: transactions
    ↓
TransactionConsumer (batch processing)
    ↓
EntityResolver (deduplication)
    ↓
EventParser (node/edge extraction)
    ↓
BatchGraphLoader (Neo4j upsert)
```

**Key Features:**
- Batches transactions (configurable batch size)
- Entity resolution for handling duplicates
- Idempotent graph operations using MERGE
- Error handling for invalid transactions

### 2. Graph Builder Layer

**Components:**
- `BatchGraphLoader` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\graph_builder\batch_loader.py" />)
- `EntityResolver` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\graph_builder\entity_resolution.py" />)
- `TTLManager` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\graph_builder\ttl_manager.py" />)
- Graph Models (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\graph_builder\models.py" />)

**Graph Schema:**
```
Nodes:
- Account (user accounts)
- Device (devices used)
- IP (IP addresses)
- Merchant (merchants transacted with)
- Card (payment cards)
- RingCandidate (detected fraud rings)

Relationships:
- Account -[USED]-> Device
- Account -[USED]-> IP
- Account -[TRANSACTED_WITH]-> Merchant
- Account -[OWNS]-> Card
- Device -[SEEN_AT]-> IP
- Account -[BELONGS_TO_RING]-> RingCandidate
```

### 3. Feature Computation Layer

**Components:**
- `StructuralFeatureCalculator` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\features\structural.py" />)
- `TemporalFeatureCalculator` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\features\temporal.py" />)
- `FeatureCache` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\serving\feature_cache.py" />)

**Structural Features:**
- Device account count
- IP account count
- Merchant account diversity
- Triangle count
- Clustering coefficient
- Connected component size
- Shared entity count

**Temporal Features:**
- New edges in last hour/day
- Edge formation burstiness

**Feature Caching:**
- Redis-based caching for precomputed features
- TTL-based cache invalidation
- Batch feature computation for efficiency

### 4. Ring Score Calculation

**Component:**
- `RingScoreCalculator` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\serving\ring_score.py" />)

**Score Composition:**
```
Ring Score = 0.7 × Unsupervised Component + 0.3 × Structural Component

Unsupervised Component:
- Graph density around account
- Connected entity patterns

Structural Component:
- Normalized device/IP account counts
- Merchant diversity
- Triangle count and clustering coefficient
- Connected component size
- Temporal edge formation patterns
```

### 5. API Layer

**Components:**
- `Ring Score API` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\serving\api.py" />)
- `Investigator API` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\serving\investigator_api.py" />)
- `CircuitBreaker` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\serving\circuit_breaker.py" />)
- `Fallback` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\serving\fallback.py" />)

**API Endpoints:**
```
Ring Score API (Port 8000/8001):
- GET /ring-score/{account_id}
- GET /circuit/status
- POST /circuit/reset

Investigator API (Port 8002):
- GET /rings/pending?limit=50
- GET /rings/{ring_id}
- POST /rings/confirm
- POST /rings/reject
- GET /rings/labels/export
```

**Resilience Features:**
- Circuit breaker for Neo4j failures
- Redis caching for low-latency lookups
- Fallback to tabular-only scoring
- Health check endpoints

### 6. Community Detection Layer

**Component:**
- `CommunityDetector` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\community\detection.py" />)
- `BatchJob` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\community\batch_job.py" />)

**Detection Methods:**
- Connected Components (basic ring detection)
- Louvain Communities (advanced clustering)

**Risk Scoring:**
```
Community Risk = 0.4 × Density + 0.3 × Clustering + 0.2 × Max Degree + 0.1 × Avg Degree
```

**Output:**
- Candidate rings with risk scores
- Ring member accounts
- Structural metrics for each ring

### 7. Embedding Layer

**Components:**
- `GraphSAGE Training` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\embeddings\train_graphsage.py" />)
- `EmbeddingInference` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\embeddings\inference.py" />)
- `SupervisedClassifier` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\embeddings\supervised_classifier.py" />)

**Graph Neural Network Pipeline:**
```
Graph Data → GraphSAGE Training → Account Embeddings → Supervised Classifier → Ring Membership Probability
```

**Features:**
- Node embeddings using GraphSAGE
- Supervised classification for fraud ring membership
- Distance to known fraud embeddings
- Cached embedding inference

### 8. Fusion Layer

**Component:**
- `LearnedFusion` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\fusion\learned_fusion.py" />)
- `DecisionPolicy` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\config\decision_policy.py" />)

**Fusion Strategies:**

1. **Manual Override Rule:**
   - Confirmed ring with ring_score > 0.90 and ≥2 members → INVESTIGATE
   - Otherwise: combined_score = α × tabular_prob + (1-α) × ring_score

2. **Learned Fusion Model:**
   - PyTorch neural network combining tabular and graph signals
   - Trained on investigator labels
   - Provides lift over manual rule

**Decision Thresholds:**
```
combined_score > 0.90 → DECLINE
0.50 < combined_score ≤ 0.90 → CHALLENGE
combined_score ≤ 0.50 → APPROVE
```

### 9. Shadow Mode

**Component:**
- `ShadowMode` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\deployment\shadow_mode.py" />)

**Purpose:**
- Sample production traffic (configurable rate)
- Compare shadow vs production decisions
- Publish comparison records to Kafka
- Enable safe model evaluation

**Flow:**
```
Kafka: transactions (sampled)
    ↓
Shadow Mode Processing
    ↓
Graph + Fusion Decisions
    ↓
Kafka: shadow_decisions (comparison records)
```

### 10. Monitoring Layer

**Component:**
- `DriftDetection` (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\src\monitoring\drift_detection.py" />)

**Monitoring Metrics:**
- Ring precision/recall
- Time to ring detection
- ROC-AUC and PR-AUC
- Fusion model lift
- Graph query latency (p50, p95, p99)
- Redis cache hit rate
- Feature distribution drift
- Investigator queue volume

## Data Flow Diagram

```
┌──────────────────┐
│ Transaction Event│
│    (Kafka)       │
└────────┬─────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│              Ingestion & Graph Building                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │   Consumer   │→ │ Event Parser │→ │ Graph Loader │     │
│  └──────────────┘  └──────────────┘  └──────────────┘     │
└──────────────────────────┬──────────────────────────────────┘
                           │
                           ▼
                    ┌──────────────┐
                    │    Neo4j     │
                    │   Graph DB   │
                    └──────┬───────┘
                           │
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│  Real-time    │  │   Batch       │  │   Training    │
│  Scoring      │  │   Community   │  │   Pipeline    │
│               │  │   Detection   │  │               │
│ ┌───────────┐ │  │ ┌───────────┐ │  │ ┌───────────┐ │
│ │Ring Score │ │  │ │Connected  │ │  │ │GraphSAGE  │ │
│ │Calculator │ │  │ │Components │ │  │ │Training   │ │
│ └───────────┘ │  │ └───────────┘ │  │ └───────────┘ │
│ ┌───────────┐ │  │ ┌───────────┐ │  │ ┌───────────┐ │
│ │Feature    │ │  │ │Louvain    │ │  │ │Supervised │ │
│ │Cache      │ │  │ │Communities│ │  │ │Classifier │ │
│ └───────────┘ │  │ └───────────┘ │  │ └───────────┘ │
└───────┬───────┘  └───────┬───────┘  └───────┬───────┘
        │                  │                  │
        ▼                  ▼                  ▼
┌───────────────┐  ┌───────────────┐  ┌───────────────┐
│   Ring Score  │  │   Ring        │  │   Embeddings  │
│     API       │  │   Candidates  │  │   & Model     │
└───────┬───────┘  └───────┬───────┘  └───────┬───────┘
        │                  │                  │
        └──────────────────┼──────────────────┘
                           ▼
                  ┌──────────────────┐
                  │  Fusion Layer    │
                  │  ┌────────────┐  │
                  │  │Decision    │  │
                  │  │Policy      │  │
                  │  └────────────┘  │
                  │  ┌────────────┐  │
                  │  │Learned     │  │
                  │  │Fusion Model│  │
                  │  └────────────┘  │
                  └────────┬─────────┘
                           │
                           ▼
                  ┌──────────────────┐
                  │  Final Decision  │
                  │ APPROVE/CHALLENGE│
                  │ DECLINE/INVESTIGATE│
                  └──────────────────┘
```

## Infrastructure Deployment

**Docker Services:** (<ref_file file="C:\Users\Administrator\graph-ring-fraud-detection\deployment\docker-compose.yml" />)

```
Infrastructure:
- Neo4j 5.14 (ports 7474, 7687)
- Redis 7.2 (port 6379)
- Kafka 7.5.0 (port 9092)
- Zookeeper (port 2181)

Application Services:
- graph-builder: Kafka consumer for graph construction
- ring-score-api: FastAPI service for ring scoring (ports 8000, 8001)
- investigator-api: Review interface for fraud rings (port 8002)
- monitoring-api: Drift monitoring service (port 8003)
- shadow-mode: Shadow decision comparison
- batch-community: Periodic community detection
- ttl-cleanup: Graph data lifecycle management
```

## Key Design Patterns

1. **Layered Architecture**: Clear separation between ingestion, graph building, feature computation, and serving layers

2. **Circuit Breaker Pattern**: Resilience against Neo4j failures with automatic fallback

3. **Batch Processing**: Efficient bulk operations for graph loading and feature computation

4. **Caching Strategy**: Multi-level caching (Redis for features, graph for embeddings)

5. **Shadow Mode**: Safe production evaluation without affecting live decisions

6. **Entity Resolution**: Deduplication and fuzzy matching for graph entities

7. **Community Detection**: Both real-time (connected components) and batch (Louvain) approaches

8. **Fusion Architecture**: Combines rule-based and learned approaches for fraud detection

## Technology Stack

- **Graph Database**: Neo4j 5.14 with Graph Data Science plugin
- **Cache**: Redis 7.2
- **Message Queue**: Apache Kafka 7.5.0
- **API Framework**: FastAPI
- **Machine Learning**: PyTorch (GraphSAGE embeddings)
- **Graph Processing**: NetworkX (community detection)
- **Containerization**: Docker & Docker Compose
- **Language**: Python 3.11+

## File Structure Mapping

```
src/
├── ingestion/          # Kafka consumption and event parsing
├── graph_builder/      # Graph construction and lifecycle
├── features/           # Structural and temporal feature computation
├── community/         # Fraud ring detection and investigation
├── serving/           # APIs, caching, and resilience
├── fusion/            # Tabular + graph signal fusion
├── embeddings/        # Graph neural network training/inference
└── config/            # Decision policy configuration

deployment/
├── docker-compose.yml # Service orchestration
├── Dockerfile.*       # Service-specific images
└── shadow_mode.py     # Shadow mode entry point

scripts/
├── generate_test_data.py
├── run_shadow_analysis.py
└── train_fusion.sh
```

This architecture enables real-time fraud detection by combining graph-based coordinated fraud detection with traditional tabular models, providing a comprehensive view of both individual and collective fraud patterns.