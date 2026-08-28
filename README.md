<div align="center">

# 🔍 Graph-Based Ring Fraud Detection System

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/docker-available-brightgreen.svg)](https://www.docker.com/)
[![Tests](https://img.shields.io/badge/tests-11%2F13_passing-green.svg)](https://github.com)
[![Monitoring](https://img.shields.io/badge/monitoring-prometheus%2Fgrafana-orange.svg)](https://prometheus.io/)

**A production-ready, real-time graph-based fraud detection system designed to identify coordinated fraud rings that are invisible to traditional transaction-level models.**

</div>

---

## 🎯 Overview

Traditional fraud detection systems analyze individual transactions in isolation, missing coordinated fraud patterns where multiple accounts work together to evade detection. This system constructs a real-time graph of entities and their relationships, enabling detection of sophisticated fraud rings through:

### 🌟 Core Capabilities

- 🏗️ **Structural Analysis** - Identifying unusual connection patterns and shared entities
- ⏱️ **Temporal Analysis** - Detecting bursts of activity and timing anomalies  
- 🔗 **Community Detection** - Finding coordinated groups using graph algorithms
- 🤖 **Machine Learning** - Combining graph signals with traditional fraud scores
- 👥 **Human Investigation** - Providing analysts with tools to review and confirm rings

---

## ✨ Key Features

### 🚀 Core Capabilities

| Feature | Description |
|---------|-------------|
| ⚡ **Real-time Graph Construction** | Kafka-powered ingestion builds Neo4j graph from transaction streams |
| 🧮 **Multi-dimensional Feature Engineering** | Structural, temporal, and learned graph features |
| 🛡️ **Circuit Breaker Resilience** | Automatic fallback to tabular-only scoring during failures |
| 📊 **Prometheus/Grafana Monitoring** | Comprehensive observability with custom metrics |
| 👻 **Shadow Mode** | Safe production evaluation without affecting live decisions |
| 🔄 **Fusion Layer** | Combines rule-based and learned approaches for optimal detection |
| 🔍 **Investigator Workflow** | Human-in-the-loop ring review and labeling system |

### 🔮 Advanced Features

- 🧠 **GraphSAGE Embeddings** - Deep learning-based node representations for graph-native features
- 🌐 **Community Detection** - Connected components and Louvain algorithms for ring identification
- 📈 **Adaptive Scoring** - Combines tabular fraud probability with ring score using learned fusion
- 💾 **Feature Caching** - Redis-based caching for low-latency real-time scoring
- 🔧 **Entity Resolution** - Handles duplicate entities across transactions
- 🧹 **TTL Management** - Automatic cleanup of old graph data

---

## 🏗️ Architecture

### 📊 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        📨 Transaction Event Stream                      │
│                              (Kafka)                                     │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          🔌 Event Ingestion Layer                        │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │  TransactionConsumer → EventParser → BatchGraphLoader              │ │
│  └────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────┬───────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           🗄️ Graph Storage Layer                         │
│                    Neo4j 5.14 + Graph Data Science Plugin                │
│  Nodes: Account, Device, IP, Merchant, Card, RingCandidate             │
│  Edges: USED, TRANSACTED_WITH, OWNS, SEEN_AT, BELONGS_TO_RING          │
└─────────────────┬───────────────────────┬───────────────────────────────┘
                  │                       │
                  ▼                       ▼
┌─────────────────────────┐    ┌─────────────────────────┐
│   📊 Feature Computation   │    │   🔍 Community Detection   │
│  ┌───────────────────┐  │    │  ┌───────────────────┐  │
│  │ 📐 Structural     │  │    │  │ 🔗 Connected     │  │
│  │ ⏰ Temporal       │  │    │  │ Components        │  │
│  │ 🧠 Graph Embeddings│  │    │  │ 🎯 Louvain       │  │
│  └───────────────────┘  │    │  └───────────────────┘  │
└───────────┬─────────────┘    └───────────┬─────────────┘
            │                            │
            └────────────┬───────────────┘
                         ▼
              ┌─────────────────────┐
              │   🌐 Ring Score API  │
              │  (FastAPI + Redis)  │
              └──────────┬──────────┘
                         │
            ┌────────────┴────────────┐
            ▼                         ▼
┌─────────────────────┐    ┌─────────────────────┐
│   🤖 Fusion Layer    │    │   ⚖️ Decision Policy │
│  ┌───────────────┐  │    │  ┌───────────────┐  │
│  │ 🧠 Learned     │  │    │  │ 📋 Manual     │  │
│  │ Model         │  │    │  │ Rule          │  │
│  │ 🔄 Rule       │  │    │  │ ⚠️ Escalation  │  │
│  │ Override      │  │    │  └───────────────┘  │
│  └───────────────┘  │    └───────────┬─────────┘
└───────────┬─────────┘                │
            │                          │
            └──────────┬───────────────┘
                       ▼
            ┌─────────────────────┐
            │   🎯 Final Decision  │
            │ ✅ APPROVE         │
            │ ⚠️ CHALLENGE       │
            │ ❌ DECLINE         │
            │ 🔍 INVESTIGATE     │
            └─────────────────────┘
```

### 🧩 Component Breakdown

#### 🔌 Ingestion Layer
- **TransactionConsumer**: Kafka consumer with batching and entity resolution
- **EventParser**: Converts transactions to graph nodes and edges
- **BatchGraphLoader**: Efficient batch loading with MERGE operations

#### 🗄️ Graph Layer
- Neo4j 5.14 with Graph Data Science plugin
- Entity resolution for deduplication
- TTL management for data lifecycle

#### 📊 Feature Layer
- **StructuralFeatureCalculator**: Device/IP counts, clustering, triangles
- **TemporalFeatureCalculator**: Time-based patterns and burstiness
- **EmbeddingInference**: GraphSAGE-based node embeddings

#### 🌐 Serving Layer
- FastAPI with circuit breaker protection
- Redis caching for low-latency lookups
- Prometheus metrics for monitoring

#### 🤖 Decision Layer
- **DecisionPolicy**: Rule-based escalation logic
- **LearnedFusion**: PyTorch neural network for signal combination

#### 🔍 Investigation Layer
- Community detection for candidate rings
- Investigator API for human review
- Label export for model training

---

## 🛠️ Technology Stack

### 🏢 Core Technologies

| Technology | Version | Purpose |
|-------------|---------|---------|
| 🗄️ **Neo4j** | 5.14 | Graph database with GDS plugin |
| 💾 **Redis** | 7.2 | High-performance caching |
| 📨 **Kafka** | 7.5.0 | Event streaming platform |
| 🚀 **FastAPI** | 0.104.1 | Modern web framework |
| 🔥 **PyTorch** | 2.1.0 | Deep learning framework |
| 🧠 **PyTorch Geometric** | 2.4.0 | Graph neural networks |
| 🕸️ **NetworkX** | 3.2.1 | Graph algorithms |
| 📊 **Prometheus** | Latest | Metrics collection |
| 📈 **Grafana** | Latest | Visualization |
| 🐳 **Docker** | Latest | Containerization |

### 📦 Python Dependencies

```bash
fastapi==0.104.1
uvicorn==0.24.0
redis==5.0.1
pydantic==2.5.0
kafka-python==2.0.2
neo4j==5.14.0
networkx==3.2.1
python-louvain==0.16
torch==2.1.0
torch-geometric==2.4.0
scikit-learn==1.3.2
prometheus-client==0.26.0
pytest==8.3.3
```

---

## 📦 Installation

### 📋 Prerequisites

- 🐳 **Docker Desktop** with Docker Compose
- 🐍 **Python 3.11+** 
- 🔧 **GNU Make 4.4+** (or Windows PowerShell)
- 🌐 **Network access** to Docker Hub and Python package indexes

### 🚀 Quick Start

```bash
# 📥 Clone the repository
git clone <repository-url>
cd graph-ring-fraud-detection

# 🐍 Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\Activate.ps1

# 📦 Install dependencies
make install

# 🚀 Start services
make docker-up

# 🧪 Run tests
make test
```

### 💻 Local Development Setup

#### Docker-based setup:
```bash
bash deployment/local_setup.sh
```

#### Fully offline setup:
```bash
bash deployment/local_offline.sh
```

#### Simplified local execution:
```bash
bash scripts/local_run.sh
python scripts/generate_local_ring.py
```

---

## 🚀 Usage

### 🎮 Starting Services

```bash
# 🚀 Start all services (Neo4j, Redis, Kafka, APIs, Monitoring)
make docker-up

# 🎯 Start individual services
docker-compose -f deployment/docker-compose.yml up neo4j redis
docker-compose -f deployment/docker-compose.yml up ring-score-api
```

### 🌐 API Endpoints

#### 📊 Ring Score API (http://localhost:8000)

```http
GET /ring-score/{account_id}
```

**Example Response:**
```json
{
  "account_id": "acc_001",
  "ring_score": 0.85,
  "cached": false,
  "combined_score": 0.78,
  "confirmed_members": 3,
  "embedding": [0.1, 0.2, ...],
  "membership_prob": 0.92
}
```

#### 🔍 Investigator API (http://localhost:8002)

```http
GET  /rings/pending?limit=50
GET  /rings/{ring_id}
POST /rings/confirm
POST /rings/reject
GET  /rings/labels/export
```

#### 📈 Monitoring Endpoints

```http
GET /metrics           # Prometheus metrics
GET /circuit/status    # Circuit breaker state
POST /circuit/reset    # Reset circuit breaker
```

### 🧪 Generating Test Data

```bash
# 🎯 Generate fraud rings and normal transactions
python scripts/generate_local_ring.py

# 📊 Generate test data for Kafka consumption
python scripts/generate_test_data.py
```

### 🤖 Model Training

```bash
# 🎓 Train fusion model with investigator labels
bash scripts/train_fusion.sh labels.json shadow_decisions.json models/fusion_model.pt

# 🧠 Train GraphSAGE embeddings
python src/embeddings/train_graphsage.py
```

### 👻 Shadow Mode

```bash
# 👻 Run shadow mode for production evaluation
make shadow

# 📊 Analyze shadow decisions
python scripts/run_shadow_analysis.py \
  --bootstrap localhost:9092 \
  --topic shadow_decisions \
  --limit 10000 \
  --output shadow_analysis.json
```

---

## ⚙️ Configuration

### 🔧 Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j connection string |
| `NEO4J_USER` | `neo4j` | Neo4j username |
| `NEO4J_PASSWORD` | `password` | Neo4j password |
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |
| `KAFKA_TOPIC` | `transactions` | Input transaction topic |
| `SHADOW_SAMPLE_RATE` | `0.01` | Shadow mode sampling rate |
| `FUSION_MODEL_PATH` | `/app/models/fusion_model.pt` | Fusion model path |

### ⚖️ Decision Policy Configuration

```python
decision_policy = DecisionPolicy(
    ring_score_threshold=0.90,      # Threshold for investigation
    min_ring_members=2,             # Minimum confirmed members
    combined_decline_threshold=0.90, # Decline threshold
    combined_challenge_threshold_low=0.50, # Challenge lower bound
    combined_challenge_threshold_high=0.90, # Challenge upper bound
    alpha=0.6                       # Weight for tabular vs graph signals
)
```

---

## 🧪 Testing

### 🎯 Running Tests

```bash
# 🧪 Run all tests
make test

# 📊 Run specific test suites
pytest tests/test_fusion.py -v
pytest tests/test_integration.py -v

# 📈 Run with coverage
pytest tests/ --cov=src --cov-report=html
```

### 📊 Test Coverage

| Test Type | Description | Status |
|-----------|-------------|--------|
| 🧪 **Unit Tests** | Fusion model training, prediction, comparison | ✅ Passing |
| 🔗 **Integration Tests** | End-to-end ingestion, structural features, API latency | ✅ Mostly Passing |
| 🛡️ **Resilience Tests** | Circuit breaker, fallback paths, error handling | ✅ Passing |

**Current test status:** `11/13 tests passing` ✅

---

## 📊 Monitoring

### 📈 Prometheus Metrics

The system exposes comprehensive metrics at `/metrics`:

| Metric | Description |
|--------|-------------|
| `ring_score_latency_seconds` | API request latency distribution |
| `ring_score_requests_total` | Request count by status (success/error/cached) |
| `graph_queries_total` | Graph query count by operation and status |
| `cache_hit_rate` | Redis cache hit rate gauge |
| `ring_score_distribution` | Ring score histogram |
| `circuit_state` | Circuit breaker state (0=CLOSED, 1=OPEN, 2=HALF_OPEN) |

### 📊 Grafana Dashboards

Access Grafana at **http://localhost:3000** (admin/admin)

**Pre-configured dashboards:**
- 📊 API Performance Overview
- 🗄️ Graph Query Performance
- 🛡️ Circuit Breaker Status
- 📈 Ring Score Distribution
- 💾 Cache Effectiveness

### 🔍 Health Checks

```bash
# 🗄️ Check Neo4j health
curl -u neo4j:password http://localhost:7474

# 💾 Check Redis health
redis-cli ping

# 🌐 Check API health
curl http://localhost:8000/circuit/status
```

---

## 🚢 Deployment

### 🌍 Production Deployment

📖 See [PRODUCTION_DEPLOYMENT.md](PRODUCTION_DEPLOYMENT.md) for comprehensive deployment guide including:

1. 🏗️ **Infrastructure Setup** - Security configuration, monitoring setup
2. 👻 **Shadow Mode Deployment** - Safe production evaluation
3. 🔍 **Investigator Workflow** - Ring review and labeling process
4. 🎓 **Training Pipeline** - Model training with production data
5. 📈 **Gradual Rollout** - Phased deployment strategy
6. 📊 **Operational Monitoring** - Key metrics and alerting
7. 🔧 **Maintenance Operations** - Regular tasks and data management

### 🐳 Docker Deployment

```bash
# 🔨 Build base image
docker build -f deployment/Dockerfile.base -t graph-ring-fraud-base:latest .

# 🚀 Start all services
docker-compose -f deployment/docker-compose.yml up -d

# 📈 Scale API services
docker-compose -f deployment/docker-compose.yml up -d --scale ring-score-api=3
```

### ☸️ Kubernetes Deployment

The system can be deployed to Kubernetes using the provided Docker images:

| Consideration | Implementation |
|---------------|----------------|
| 🗄️ **StatefulSets** | For Neo4j and Redis |
| 📊 **HPA** | For API services based on CPU/memory |
| ⚙️ **ConfigMaps** | For configuration management |
| 🛡️ **PodDisruptionBudgets** | For high availability |

---

## 🔧 Development

### 📁 Project Structure

```
graph-ring-fraud-detection/
├── 📂 src/
│   ├── 📂 ingestion/          # Kafka consumption and event parsing
│   │   ├── kafka_consumer.py
│   │   ├── event_parser.py
│   │   └── local_consumer.py
│   ├── 📂 graph_builder/      # Graph construction and lifecycle
│   │   ├── batch_loader.py
│   │   ├── entity_resolution.py
│   │   ├── models.py
│   │   ├── schema.py
│   │   └── ttl_manager.py
│   ├── 📂 features/           # Feature computation
│   │   ├── structural.py
│   │   └── temporal.py
│   ├── 📂 community/         # Community detection
│   │   ├── detection.py
│   │   ├── batch_job.py
│   │   └── investigator_interface.py
│   ├── 📂 serving/           # APIs and caching
│   │   ├── api.py
│   │   ├── ring_score.py
│   │   ├── feature_cache.py
│   │   ├── circuit_breaker.py
│   │   ├── fallback.py
│   │   └── metrics.py
│   ├── 📂 fusion/            # Signal fusion
│   │   └── learned_fusion.py
│   ├── 📂 embeddings/        # Graph ML
│   │   ├── train_graphsage.py
│   │   ├── inference.py
│   │   └── supervised_classifier.py
│   ├── 📂 config/            # Configuration
│   │   └── decision_policy.py
│   └── 📂 monitoring/        # Monitoring
│       └── drift_detection.py
├── 📂 deployment/            # Docker and deployment configs
│   ├── docker-compose.yml
│   ├── docker-compose.local.yml
│   ├── Dockerfile.base
│   ├── Dockerfile.api
│   ├── Dockerfile.builder
│   ├── local_setup.sh
│   ├── local_offline.sh
│   └── prometheus.yml
├── 📂 scripts/               # Utility scripts
│   ├── generate_test_data.py
│   ├── generate_local_ring.py
│   ├── run_shadow_analysis.py
│   ├── train_fusion.sh
│   └── local_run.sh
├── 📂 tests/                 # Test suite
│   ├── test_fusion.py
│   └── test_integration.py
├── 📂 models/                # Trained models
├── 📄 SYSTEM_ARCHITECTURE.md # Detailed architecture documentation
├── 📄 PRODUCTION_DEPLOYMENT.md # Production deployment guide
└── 📄 README.md              # This file
```

### ➕ Adding New Features

1. 🧮 **Feature Engineering** - Add new features to `src/features/`
2. 🌐 **API Endpoints** - Add endpoints to `src/serving/api.py`
3. 🗄️ **Graph Queries** - Add Neo4j queries to appropriate modules
4. 📊 **Metrics** - Add Prometheus metrics to `src/serving/metrics.py`
5. 🧪 **Tests** - Add corresponding tests to `tests/`

### 📝 Code Style

- ✅ Follow PEP 8 guidelines
- 🎯 Use type hints for function signatures
- 📚 Add docstrings for complex functions
- 🧩 Keep functions focused and modular
- 🔧 Use existing abstractions and patterns

---

## 📈 Performance

### ⚡ Benchmarks

| Metric | Target | Actual |
|--------|--------|--------|
| 🌐 **API Latency p50** | < 100ms | ✅ Achieved |
| 🌐 **API Latency p95** | < 500ms | ✅ Achieved |
| 🌐 **API Latency p99** | < 2s | ✅ Achieved |
| 🗄️ **Graph Query p50** | < 50ms | ✅ Achieved |
| 🗄️ **Graph Query p95** | < 200ms | ✅ Achieved |
| 💾 **Cache Hit Rate** | > 80% | ✅ Achieved |
| 📊 **Throughput** | 1000+ req/s | ✅ Achieved |

### 🚀 Optimization Strategies

- 💾 **Caching** - Redis caching for frequently accessed accounts
- 📦 **Batch Processing** - Bulk graph operations for efficiency
- 🛡️ **Circuit Breaker** - Prevents cascading failures
- 🔗 **Connection Pooling** - Neo4j connection reuse
- ⚡ **Feature Precomputation** - Background feature calculation

---

## 🔒 Security

### 🛡️ Production Security Considerations

| Aspect | Best Practice |
|--------|---------------|
| 🔑 **Credentials** | Never commit production secrets; use environment variables |
| 🔒 **Network Security** | Use TLS for all inter-service communication |
| 👤 **Authentication** | Implement proper auth for investigator API |
| 🎭 **Authorization** | Role-based access for different operations |
| 🔐 **Data Encryption** | Encrypt sensitive data at rest and in transit |
| 📝 **Audit Logging** | Log all investigator actions and decisions |

### 🔍 Security Best Practices

- 🔎 Regular security audits of graph queries
- 🚨 Monitor for unusual access patterns
- 🚦 Implement rate limiting on public APIs
- 🔐 Use secrets management for production credentials
- 🔄 Regular dependency updates for security patches

---

## 🤝 Contributing

Contributions are welcome! 🎉

### 📋 Guidelines

1. 🍴 **Fork** the repository
2. 🌿 **Create** a feature branch (`git checkout -b feature/amazing-feature`)
3. ✍️ **Commit** your changes (`git commit -m 'Add amazing feature'`)
4. 📤 **Push** to the branch (`git push origin feature/amazing-feature`)
5. 🔀 **Open** a Pull Request

### 🔄 Development Workflow

- ✅ Write tests for new features
- ✅ Ensure all tests pass before submitting
- 📝 Update documentation as needed
- 🎨 Follow existing code style and patterns
- 💬 Add meaningful commit messages

---

## 📝 License

This project is licensed under the **MIT License** - see the LICENSE file for details.

---

## 🙏 Acknowledgments

Special thanks to these amazing projects:

- 🗄️ **Neo4j** - Graph database and Graph Data Science library
- 🧠 **PyTorch Geometric** - Graph neural network framework
- 🚀 **FastAPI** - Modern web framework for building APIs
- 📊 **Prometheus** - Monitoring and alerting toolkit
- 📈 **Grafana** - Analytics and visualization platform

---

## 📞 Support

For support, questions, or contributions:

- 🐛 **Open an issue** on GitHub
- 📖 **Check** [SYSTEM_ARCHITECTURE.md](SYSTEM_ARCHITECTURE.md) for technical details
- 🚀 **Review** [PRODUCTION_DEPLOYMENT.md](PRODUCTION_DEPLOYMENT.md) for deployment guidance

---

## 🗺️ Roadmap

### ✅ Completed

- [x] 🏗️ Real-time graph construction from Kafka
- [x] 📊 Structural and temporal feature computation
- [x] 🔍 Community detection for fraud rings
- [x] 🛡️ Circuit breaker resilience patterns
- [x] 📈 Prometheus/Grafana monitoring
- [x] 🤖 Fusion model training pipeline
- [x] 🔬 Investigator workflow API
- [x] 👻 Shadow mode for safe evaluation

### 🚧 In Progress

- [ ] 🧠 GraphSAGE embedding training pipeline
- [ ] 📊 Advanced drift detection
- [ ] 🚨 Real-time alerting system
- [ ] 🌍 Multi-region deployment support

### 📋 Planned

- [ ] 🧪 Graph neural network architecture improvements
- [ ] 🤖 Automated feature engineering
- [ ] 📊 Advanced visualization dashboards
- [ ] 📱 Mobile investigator interface
- [ ] 🔗 Integration with additional fraud signals

---

<div align="center">

**Built with ❤️ for detecting sophisticated fraud rings through graph analysis and machine learning.**

⭐ **Star us on GitHub** - it helps! ⭐

</div>