# Deployment Guide

Guidelines for deploying Job Orchestrator in various environments.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Single Node Deployment](#single-node-deployment)
- [Multi-Node Deployment](#multi-node-deployment)
- [Docker Deployment](#docker-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Monitoring](#monitoring)
- [Security](#security)
- [Performance Tuning](#performance-tuning)
- [Troubleshooting](#troubleshooting)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                     Application Layer                        │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │   Web App   │  │   API       │  │   CLI       │          │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘          │
└─────────┼────────────────┼────────────────┼─────────────────┘
          │                │                │
          └────────────────┼────────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Job Orchestrator                          │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │  Scheduler  │──│ WorkerPool  │──│     DLQ     │          │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘          │
│         │                │                │                  │
│  ┌──────┴──────┐  ┌──────┴──────┐  ┌──────┴──────┐          │
│  │PriorityQueue│  │   Workers   │  │   Locking   │          │
│  └──────┬──────┘  └─────────────┘  └──────┬──────┘          │
└─────────┼────────────────────────────────┼──────────────────┘
          │                                │
          └────────────────┬───────────────┘
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    Storage Layer                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐          │
│  │    Redis    │  │ PostgreSQL  │  │   SQLite    │          │
│  └─────────────┘  └─────────────┘  └─────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

### Deployment Models

| Model | Workers | Storage | Locking | Use Case |
|-------|---------|---------|---------|----------|
| Embedded | In-process | Memory | Memory | Development, testing |
| Single Node | Threads/Processes | SQLite/Redis | Memory/File | Small production |
| Multi-Node | Processes | Redis/PostgreSQL | Redis | Scalable production |

---

## Single Node Deployment

### Basic Setup

```python
# app.py
from job_orchestrator import OrchestratorConfig
from job_orchestrator.scheduler import Scheduler
from job_orchestrator.workers import WorkerPool, PoolConfig, WorkerType

def main():
    # Load configuration
    config = OrchestratorConfig.from_yaml("config.yaml")
    
    # Create scheduler
    scheduler = Scheduler(config)
    
    # Create worker pool
    pool_config = PoolConfig(
        min_workers=4,
        max_workers=8,
        worker_type=WorkerType.THREAD,
    )
    pool = WorkerPool(scheduler, pool_config)
    
    # Start
    scheduler.start()
    pool.start()
    
    print("Job Orchestrator running...")
    
    try:
        # Keep running
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
        pool.stop(wait=True)
        scheduler.stop(wait=True)

if __name__ == "__main__":
    main()
```

### Configuration for Single Node

```yaml
# config.yaml
worker_pool:
  min_workers: 4
  max_workers: 8
  worker_type: thread

storage:
  backend: sqlite
  connection_string: sqlite:///jobs.db

locking:
  backend: file
  connection_string: /var/lib/job-orchestrator/locks

logging:
  level: INFO
  file: /var/log/job-orchestrator/app.log
```

### Systemd Service

```ini
# /etc/systemd/system/job-orchestrator.service
[Unit]
Description=Job Orchestrator Service
After=network.target

[Service]
Type=simple
User=joborch
Group=joborch
WorkingDirectory=/opt/job-orchestrator
Environment="PYTHONPATH=/opt/job-orchestrator"
ExecStart=/opt/job-orchestrator/venv/bin/python app.py
ExecStop=/bin/kill -SIGTERM $MAINPID
Restart=always
RestartSec=5
TimeoutStopSec=30

# Resource limits
LimitNOFILE=65536
LimitNPROC=4096

# Security
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/job-orchestrator /var/log/job-orchestrator

[Install]
WantedBy=multi-user.target
```

```bash
# Install and start
sudo systemctl daemon-reload
sudo systemctl enable job-orchestrator
sudo systemctl start job-orchestrator
sudo systemctl status job-orchestrator
```

---

## Multi-Node Deployment

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       Load Balancer                          │
└─────────────────────────┬───────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          ▼               ▼               ▼
    ┌───────────┐   ┌───────────┐   ┌───────────┐
    │  Node 1   │   │  Node 2   │   │  Node 3   │
    │ Scheduler │   │ Scheduler │   │ Scheduler │
    │ + Workers │   │ + Workers │   │ + Workers │
    └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
          │               │               │
          └───────────────┼───────────────┘
                          ▼
    ┌─────────────────────────────────────────────────────────┐
    │                    Shared Storage                        │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
    │  │ Redis       │  │ PostgreSQL  │  │ Redis       │      │
    │  │ (Locking)   │  │ (Storage)   │  │ (Queue)     │      │
    │  └─────────────┘  └─────────────┘  └─────────────┘      │
    └─────────────────────────────────────────────────────────┘
```

### Configuration

```yaml
# config.multi-node.yaml
worker_pool:
  min_workers: 4
  max_workers: 16
  worker_type: process

storage:
  backend: postgresql
  connection_string: postgresql://joborch:password@postgres:5432/jobs
  pool_size: 20

locking:
  backend: redis
  connection_string: redis://redis:6379/0
  default_ttl: 60.0

logging:
  level: INFO
  json_format: true

metrics:
  enabled: true
  exporter: prometheus
```

### Leader Election

For multi-node deployments, only one node should manage DAG execution and cleanup:

```python
from job_orchestrator.locking import RedisLockManager
import time

def run_as_leader(lock_manager, scheduler):
    """Run leader responsibilities."""
    while scheduler.is_running:
        # Leader tasks
        scheduler.cleanup_expired_jobs()
        scheduler.check_dag_progress()
        time.sleep(10)

def main():
    lock_manager = RedisLockManager(redis_url="redis://redis:6379/0")
    scheduler = Scheduler(config)
    pool = WorkerPool(scheduler)
    
    scheduler.start()
    pool.start()
    
    while scheduler.is_running:
        # Try to become leader
        lock = lock_manager.acquire(
            "job_orchestrator:leader",
            owner=f"node-{NODE_ID}",
            ttl=30.0,
        )
        
        if lock:
            try:
                run_as_leader(lock_manager, scheduler)
            finally:
                lock_manager.release("job_orchestrator:leader", f"node-{NODE_ID}")
        else:
            # Not leader, just process jobs
            time.sleep(5)
```

---

## Docker Deployment

### Dockerfile

```dockerfile
# Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd --create-home --shell /bin/bash joborch && \
    chown -R joborch:joborch /app

USER joborch

# Default environment
ENV PYTHONUNBUFFERED=1 \
    JOB_ORCHESTRATOR_LOGGING_LEVEL=INFO \
    JOB_ORCHESTRATOR_LOGGING_JSON_FORMAT=true

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"

# Run
CMD ["python", "app.py"]
```

### Docker Compose

```yaml
# docker-compose.yaml
version: '3.8'

services:
  orchestrator:
    build: .
    environment:
      - JOB_ORCHESTRATOR_STORAGE_BACKEND=postgresql
      - JOB_ORCHESTRATOR_STORAGE_CONNECTION_STRING=postgresql://joborch:password@postgres:5432/jobs
      - JOB_ORCHESTRATOR_LOCKING_BACKEND=redis
      - JOB_ORCHESTRATOR_LOCKING_CONNECTION_STRING=redis://redis:6379/0
      - JOB_ORCHESTRATOR_WORKER_POOL_MIN_WORKERS=2
      - JOB_ORCHESTRATOR_WORKER_POOL_MAX_WORKERS=8
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    deploy:
      replicas: 3
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '0.5'
          memory: 512M
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"]
      interval: 30s
      timeout: 10s
      retries: 3
    networks:
      - orchestrator-network

  postgres:
    image: postgres:15-alpine
    environment:
      POSTGRES_USER: joborch
      POSTGRES_PASSWORD: password
      POSTGRES_DB: jobs
    volumes:
      - postgres-data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U joborch -d jobs"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - orchestrator-network

  redis:
    image: redis:7-alpine
    command: redis-server --appendonly yes
    volumes:
      - redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - orchestrator-network

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus-data:/prometheus
    ports:
      - "9090:9090"
    networks:
      - orchestrator-network

  grafana:
    image: grafana/grafana:latest
    volumes:
      - grafana-data:/var/lib/grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    networks:
      - orchestrator-network

volumes:
  postgres-data:
  redis-data:
  prometheus-data:
  grafana-data:

networks:
  orchestrator-network:
    driver: bridge
```

### Build and Run

```bash
# Build image
docker build -t job-orchestrator:latest .

# Run with docker-compose
docker-compose up -d

# Scale workers
docker-compose up -d --scale orchestrator=5

# View logs
docker-compose logs -f orchestrator

# Stop
docker-compose down
```

---

## Kubernetes Deployment

### Namespace and ConfigMap

```yaml
# k8s/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: job-orchestrator

---
# k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: orchestrator-config
  namespace: job-orchestrator
data:
  config.yaml: |
    worker_pool:
      min_workers: 2
      max_workers: 8
      worker_type: thread
      scale_up_threshold: 0.8
      scale_down_threshold: 0.2
    
    retry:
      max_retries: 5
      base_delay: 1.0
    
    dlq:
      max_size: 10000
      ttl_days: 30
    
    logging:
      level: INFO
      json_format: true
    
    metrics:
      enabled: true
      exporter: prometheus
```

### Secrets

```yaml
# k8s/secrets.yaml
apiVersion: v1
kind: Secret
metadata:
  name: orchestrator-secrets
  namespace: job-orchestrator
type: Opaque
stringData:
  postgres-url: postgresql://joborch:password@postgres:5432/jobs
  redis-url: redis://redis:6379/0
```

### Deployment

```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: job-orchestrator
  namespace: job-orchestrator
spec:
  replicas: 3
  selector:
    matchLabels:
      app: job-orchestrator
  template:
    metadata:
      labels:
        app: job-orchestrator
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      serviceAccountName: job-orchestrator
      securityContext:
        runAsNonRoot: true
        runAsUser: 1000
        fsGroup: 1000
      containers:
        - name: orchestrator
          image: job-orchestrator:latest
          imagePullPolicy: Always
          ports:
            - containerPort: 8080
              name: http
          env:
            - name: JOB_ORCHESTRATOR_STORAGE_BACKEND
              value: "postgresql"
            - name: JOB_ORCHESTRATOR_STORAGE_CONNECTION_STRING
              valueFrom:
                secretKeyRef:
                  name: orchestrator-secrets
                  key: postgres-url
            - name: JOB_ORCHESTRATOR_LOCKING_BACKEND
              value: "redis"
            - name: JOB_ORCHESTRATOR_LOCKING_CONNECTION_STRING
              valueFrom:
                secretKeyRef:
                  name: orchestrator-secrets
                  key: redis-url
            - name: NODE_ID
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
          volumeMounts:
            - name: config
              mountPath: /app/config.yaml
              subPath: config.yaml
          resources:
            requests:
              cpu: "500m"
              memory: "512Mi"
            limits:
              cpu: "2000m"
              memory: "2Gi"
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 10
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /ready
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 10
      volumes:
        - name: config
          configMap:
            name: orchestrator-config
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                labelSelector:
                  matchExpressions:
                    - key: app
                      operator: In
                      values:
                        - job-orchestrator
                topologyKey: kubernetes.io/hostname
```

### Horizontal Pod Autoscaler

```yaml
# k8s/hpa.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: job-orchestrator-hpa
  namespace: job-orchestrator
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: job-orchestrator
  minReplicas: 2
  maxReplicas: 10
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Pods
      pods:
        metric:
          name: queue_size
        target:
          type: AverageValue
          averageValue: "1000"
```

### Service

```yaml
# k8s/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: job-orchestrator
  namespace: job-orchestrator
spec:
  selector:
    app: job-orchestrator
  ports:
    - port: 8080
      targetPort: 8080
      name: http
  type: ClusterIP
```

### Deploy to Kubernetes

```bash
# Apply all manifests
kubectl apply -f k8s/

# Check deployment
kubectl -n job-orchestrator get pods
kubectl -n job-orchestrator get svc

# View logs
kubectl -n job-orchestrator logs -f deployment/job-orchestrator

# Scale manually
kubectl -n job-orchestrator scale deployment job-orchestrator --replicas=5
```

---

## Monitoring

### Health Endpoints

Implement health check endpoints in your application:

```python
from flask import Flask, jsonify
from job_orchestrator.scheduler import Scheduler

app = Flask(__name__)
scheduler = None

@app.route('/health')
def health():
    """Liveness probe - is the process alive?"""
    return jsonify({"status": "healthy"}), 200

@app.route('/ready')
def ready():
    """Readiness probe - can we accept work?"""
    if scheduler and scheduler.is_running:
        stats = scheduler.get_stats()
        return jsonify({
            "status": "ready",
            "queue_size": stats.get("queue_size", 0),
            "workers": stats.get("worker_count", 0),
        }), 200
    return jsonify({"status": "not_ready"}), 503

@app.route('/metrics')
def metrics():
    """Prometheus metrics endpoint."""
    if scheduler:
        stats = scheduler.get_stats()
        dlq_stats = scheduler.get_dlq_stats()
        
        metrics_output = []
        metrics_output.append(f'job_orchestrator_jobs_completed_total {stats.get("jobs_completed", 0)}')
        metrics_output.append(f'job_orchestrator_jobs_failed_total {stats.get("jobs_failed", 0)}')
        metrics_output.append(f'job_orchestrator_queue_size {stats.get("queue_size", 0)}')
        metrics_output.append(f'job_orchestrator_dlq_size {dlq_stats.total_entries}')
        metrics_output.append(f'job_orchestrator_worker_count {stats.get("worker_count", 0)}')
        
        return '\n'.join(metrics_output), 200, {'Content-Type': 'text/plain'}
    return '', 503
```

### Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'job-orchestrator'
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - job-orchestrator
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
        action: keep
        regex: true
      - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
        action: replace
        target_label: __metrics_path__
        regex: (.+)
      - source_labels: [__address__, __meta_kubernetes_pod_annotation_prometheus_io_port]
        action: replace
        regex: ([^:]+)(?::\d+)?;(\d+)
        replacement: $1:$2
        target_label: __address__
```

### Key Metrics to Monitor

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `jobs_completed_total` | Total completed jobs | - |
| `jobs_failed_total` | Total failed jobs | > 5% of total |
| `queue_size` | Current queue size | > 10000 |
| `dlq_size` | Dead letter queue size | > 100 |
| `worker_count` | Active workers | < min_workers |
| `job_duration_seconds` | Job execution time | p99 > 60s |
| `retry_count` | Total retries | Spike indicates issues |

### Grafana Dashboard

Import the Job Orchestrator dashboard for Grafana:

```json
{
  "dashboard": {
    "title": "Job Orchestrator",
    "panels": [
      {
        "title": "Jobs per Second",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(job_orchestrator_jobs_completed_total[5m])",
            "legendFormat": "Completed"
          },
          {
            "expr": "rate(job_orchestrator_jobs_failed_total[5m])",
            "legendFormat": "Failed"
          }
        ]
      },
      {
        "title": "Queue Size",
        "type": "gauge",
        "targets": [
          {
            "expr": "job_orchestrator_queue_size"
          }
        ]
      },
      {
        "title": "Worker Count",
        "type": "stat",
        "targets": [
          {
            "expr": "job_orchestrator_worker_count"
          }
        ]
      },
      {
        "title": "DLQ Size",
        "type": "gauge",
        "targets": [
          {
            "expr": "job_orchestrator_dlq_size"
          }
        ],
        "thresholds": {
          "steps": [
            {"color": "green", "value": 0},
            {"color": "yellow", "value": 50},
            {"color": "red", "value": 100}
          ]
        }
      }
    ]
  }
}
```

### Alerting Rules

```yaml
# alert_rules.yml
groups:
  - name: job-orchestrator
    rules:
      - alert: HighFailureRate
        expr: rate(job_orchestrator_jobs_failed_total[5m]) / rate(job_orchestrator_jobs_completed_total[5m]) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: High job failure rate
          description: "Failure rate is {{ $value | humanizePercentage }}"

      - alert: QueueBacklog
        expr: job_orchestrator_queue_size > 10000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: Large queue backlog
          description: "Queue size is {{ $value }}"

      - alert: DLQGrowing
        expr: increase(job_orchestrator_dlq_size[1h]) > 50
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: Dead letter queue growing rapidly

      - alert: NoWorkers
        expr: job_orchestrator_worker_count == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: No workers available
```

---

## Security

### Network Security

```yaml
# k8s/network-policy.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: orchestrator-network-policy
  namespace: job-orchestrator
spec:
  podSelector:
    matchLabels:
      app: job-orchestrator
  policyTypes:
    - Ingress
    - Egress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              name: monitoring
      ports:
        - protocol: TCP
          port: 8080
  egress:
    - to:
        - podSelector:
            matchLabels:
              app: postgres
      ports:
        - protocol: TCP
          port: 5432
    - to:
        - podSelector:
            matchLabels:
              app: redis
      ports:
        - protocol: TCP
          port: 6379
```

### Secret Management

Use external secret management:

```yaml
# Using External Secrets Operator
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: orchestrator-secrets
  namespace: job-orchestrator
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: ClusterSecretStore
  target:
    name: orchestrator-secrets
  data:
    - secretKey: postgres-url
      remoteRef:
        key: job-orchestrator/postgres
        property: url
    - secretKey: redis-url
      remoteRef:
        key: job-orchestrator/redis
        property: url
```

### RBAC

```yaml
# k8s/rbac.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: job-orchestrator
  namespace: job-orchestrator

---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: job-orchestrator-role
  namespace: job-orchestrator
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "list"]

---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: job-orchestrator-binding
  namespace: job-orchestrator
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: job-orchestrator-role
subjects:
  - kind: ServiceAccount
    name: job-orchestrator
    namespace: job-orchestrator
```

---

## Performance Tuning

### Worker Pool Sizing

```python
import multiprocessing
import os

# I/O-bound: More workers than CPUs
io_bound_workers = multiprocessing.cpu_count() * 4

# CPU-bound: Match CPU count
cpu_bound_workers = multiprocessing.cpu_count()

# Mixed workload
mixed_workers = multiprocessing.cpu_count() * 2
```

### Queue Tuning

```yaml
# High throughput
queue:
  max_size: 500000  # Large queue for bursts

# Low latency
queue:
  max_size: 1000    # Small queue, fast processing
```

### Database Connection Pooling

```yaml
storage:
  backend: postgresql
  pool_size: 20          # Active connections
  pool_timeout: 10.0     # Connection wait timeout
  pool_recycle: 3600     # Recycle connections hourly
```

### Redis Tuning

```bash
# redis.conf
maxmemory 2gb
maxmemory-policy allkeys-lru
tcp-keepalive 300
timeout 0
```

---

## Troubleshooting

### Common Issues

#### Jobs Not Processing

```bash
# Check scheduler status
curl http://localhost:8080/health

# Check queue size
curl http://localhost:8080/metrics | grep queue_size

# Check workers
curl http://localhost:8080/metrics | grep worker_count
```

#### High Memory Usage

```python
# Reduce queue size
config = OrchestratorConfig.from_dict({
    "queue": {"max_size": 10000},
    "dlq": {"max_size": 1000},
})
```

#### Connection Errors

```bash
# Test PostgreSQL
psql -h postgres -U joborch -d jobs -c "SELECT 1"

# Test Redis
redis-cli -h redis ping
```

#### Performance Issues

```python
# Enable profiling
import cProfile
import pstats

profiler = cProfile.Profile()
profiler.enable()

# Run scheduler
scheduler.run_job(job)

profiler.disable()
stats = pstats.Stats(profiler)
stats.sort_stats('cumulative')
stats.print_stats(20)
```

### Debug Mode

```yaml
logging:
  level: DEBUG
  format: "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
```

### Log Analysis

```bash
# Search for errors
grep -i error /var/log/job-orchestrator/app.log

# Count job failures
grep "Job failed" /var/log/job-orchestrator/app.log | wc -l

# View recent activity
tail -f /var/log/job-orchestrator/app.log | jq .