# Artifacts Crawler

## Requirements

Base:

* Docker
* Docker Compose
* JDK 8+
* Flink 1.10.0_2.12
* Kafka (in theory 0.10.x or any higher version should be fine)  

## Build and run

Clone repo and run inside it:

```shell script
# Set up cluster
FETCHED_APKS_DIR=/tmp/fetched_apks docker-compose up -d
# Build apps
./gradlew clean test shadowJar
# Deploy apps to cluster
flink run -d -p 4 crawler/build/libs/crawler-0.1-all.jar --config-file "deploy/crawler-dev.conf"
flink run -d -p 4 artifacts_fetcher/build/libs/artifacts_fetcher-0.1-all.jar --config-file "deploy/artifacts-fetcher-dev.conf"
# Populate seeds
echo '{"url":"https://www.apkmirror.com/", "ignoreExternalUrls":true}' | kafka-console-producer.sh --broker-list localhost:9092 --topic scheduledUrls
```

## Configuration 

All configurations are in `./deploy` dir