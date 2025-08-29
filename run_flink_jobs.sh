#!/bin/bash
# run_flink_jobs.sh - Proper startup order for price intelligence pipeline

echo "Starting Flink jobs in correct order..."

# 1. Start dims job FIRST (materializes CDC to Kafka topics)
echo "Starting dims materialization job..."
docker exec -d flink-jobmanager /opt/flink/bin/flink run \
  --python /opt/flink/app/jobs/dims_job.py \
  --jobmanager flink-jobmanager:8081 \
  --parallelism 1

# Wait for dims to initialize
echo "Waiting 15 seconds for dims job to initialize..."
sleep 15

# 2. Start consumer jobs (depend on dims)
echo "Starting price monitoring job..."
docker exec -d flink-jobmanager /opt/flink/bin/flink run \
  --python /opt/flink/app/jobs/price_monitoring_job.py \
  --jobmanager flink-jobmanager:8081 \
  --parallelism 1

echo "Starting alerts job..."
docker exec -d flink-jobmanager /opt/flink/bin/flink run \
  --python /opt/flink/app/jobs/alerts_job.py \
  --jobmanager flink-jobmanager:8081 \
  --parallelism 1

echo "Starting summary job..."
docker exec -d flink-jobmanager /opt/flink/bin/flink run \
  --python /opt/flink/app/jobs/summary_job.py \
  --jobmanager flink-jobmanager:8081 \
  --parallelism 1

echo "Starting user behavior job..."
docker exec -d flink-jobmanager /opt/flink/bin/flink run \
  --python /opt/flink/app/jobs/user_behavior_job.py \
  --jobmanager flink-jobmanager:8081 \
  --parallelism 1

echo "Starting demand vs signals job..."
docker exec -d flink-jobmanager /opt/flink/bin/flink run \
  --python /opt/flink/app/jobs/demand_vs_signals_job.py \
  --jobmanager flink-jobmanager:8081 \
  --parallelism 1

echo "All jobs started! Check status with: docker exec flink-jobmanager /opt/flink/bin/flink list"