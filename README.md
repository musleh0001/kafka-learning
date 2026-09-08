<div align="center">
  <h1>Kafka Learning & Monitoring</h1>
  <p>Apache Kafka KRaft cluster with Prometheus & Grafana monitoring stack</p>
</div>

---

## Architecture & Services

| Service | Port | Description | Credentials / URL |
| :--- | :--- | :--- | :--- |
| **Kafka** | `9092` (host), `29092` (internal) | Apache Kafka 4.1.0 in KRaft mode | `localhost:9092` |
| **Kafka Exporter** | `9308` | Scrapes cluster & consumer lag metrics | [http://localhost:9308/metrics](http://localhost:9308/metrics) |
| **Prometheus** | `9090` | Time-series database scraping Kafka Exporter | [http://localhost:9090](http://localhost:9090) |
| **Grafana** | `3000` | Metrics visualization dashboard | [http://localhost:3000](http://localhost:3000) (`admin` / `admin`) |

---

## Quick Start

1. **Start all services**:
   ```shell
   docker compose up -d
   ```

2. **Verify running containers**:
   ```shell
   docker compose ps
   ```

3. **Open Monitoring Dashboards**:
   - **Grafana**: [http://localhost:3000](http://localhost:3000) (Login: `admin` / `admin`). The **Kafka Overview** dashboard is pre-loaded under Dashboards.
   - **Prometheus Targets**: [http://localhost:9090/targets](http://localhost:9090/targets) (should show `kafka-exporter` in `UP` state).

---

## Kafka CLI Cheat Sheet

### Create Topics

```shell
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### List Topics

```shell
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

### Describe Topics

```shell
docker exec -it kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --describe \
  --topic orders \
  --bootstrap-server localhost:9092
```

### Produce Messages

```shell
docker exec -it kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --topic orders \
  --bootstrap-server localhost:9092
```

### Consume Messages

```shell
docker exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --topic orders \
  --from-beginning \
  --bootstrap-server localhost:9092
```

### Consumer Group list

```shell
docker exec -it kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list
```

### Inspect Consumer Group

```shell
docker exec -it kafka \
  /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --group <group_name>
```