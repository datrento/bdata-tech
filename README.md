# bdata-tech

## Start the services
bash run.sh start

## View logs
bash run.sh logs

## Stop services
bash run.sh stop

## Rebuild and start
bash run.sh build && bash run.sh start



# This method should definitely show in UI
docker exec -it flink-jobmanager /opt/flink/bin/flink run \
  --python /opt/flink/app/jobs/price_monitoring_job.py \
  --jobmanager flink-jobmanager:8081 \
  --parallelism 2