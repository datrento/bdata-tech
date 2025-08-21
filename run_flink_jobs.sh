#!/bin/bash
# run_flink_jobs.sh

# Run price monitoring job
docker exec -d flink-jobmanager /opt/flink/bin/flink run \
  --python /opt/flink/app/jobs/price_monitoring_job.py \
  --jobmanager flink-jobmanager:8081 \
  --parallelism 1

# Run undercut alerts job
docker exec -it flink-jobmanager /opt/flink/bin/flink run \
  --python /opt/flink/app/jobs/alerts_job.py \
  --jobmanager flink-jobmanager:8081 \
  --parallelism 1