# Artifacts Crawler

Test task for Kaspersky Labs.

## About
 
This is a simple web crawler designed to collect APK artifacts from www.apkmirror.com. 
The crawler is distributed and horizontally scalable. 

Apache Flink is used as a platform for distributed computing and streaming. 
Apache Kafka is used as a distributed message queue and Aerospike is used as a distributed cache.

Despite the fact that the crawler can only collect artifacts from one site, it can be easily adapted to collect any data.

## Architecture

Crawler consists of 2 streaming apps/modules + common module :

### `crawler` 

It performs recursive crawling of pages from `scheduledUrls` topic. 
New found non-artifact links are added to `scheduledUrls` topic. 
Artifact links are added to `artifact` topic.
Failed to fetch links are added to `quarantineUrls`.

Also it uses Aerospike as cache for already met urls.

### `artifacts_fetcher`

Fetches, unpacks and writes to filesystem artifacts (and metadata) from `artifact` topic.
Failed to fetch urls are streamed to `fetchQuarantineArtifacts`.
Failed to unpack artifacts (their urls) are streamed to `unpackQuarantineArtifacts`. 

## TODO

The crawler is fully functional, but this is just a test task. Therefore, the following features are missing.

* Error handling policy. Right now it just streams data to `quarantine*` topics in Kafka.
* Proxy Pool. I don't have access to any proxy pools, therefore system might work very slow. 
Is exceeds the site's request rate limit very fast.
* Distributed artifact storage. Right now fetcher just puts artifacts on file system. HDFS, S3 or some distributed FS 
should be used.
* Unpacked APKs metadata collector and storage. Right now Artifacts-fetcher just logs this metadata into file.
* `.APKM` (custom Apkmirror format) file format https://github.com/android-police/apkmirror-public/issues/113. 
There are only one open-source realisation of this format. Unfortunately, I couldn't adopt them, and don't have time
to implement it on my own.
* Integration tests
* Metrics
* Many optimizations

## Requirements

* Docker
* Docker Compose
* JDK 8+
* Flink 1.10.0_2.12
* Kafka (in theory 0.10.x or any higher version should be fine)  

## Local build and run

Clone repo and run inside it:

```shell script
# Set up a cluster
export FETCHED_APKS_DIR=/tmp/fetched_apks
# Flink's gid:uid
mkdir -p $FETCHED_APKS_DIR && sudo chown 9999:9999 $FETCHED_APKS_DIR && sudo chmod -R 777 $FETCHED_APKS_DIR
docker-compose up -d --build
# Build apps
./gradlew clean test shadowJar
# Deploy apps to cluster ('-p' stands for for parallelism)
flink run -d -p 1 crawler/build/libs/crawler-0.1-all.jar --config-file "deploy/crawler-dev.conf"
flink run -d -p 1 artifacts_fetcher/build/libs/artifacts_fetcher-0.1-all.jar --config-file "deploy/artifacts-fetcher-dev.conf"
# Populate seeds
echo '{"url":"https://www.apkmirror.com/", "ignoreExternalUrls":true}' | kafka-console-producer.sh --broker-list localhost:9092 --topic scheduledUrls
# If you want to populate only artifacts:
cat test-artifacts.txt | kafka-console-producer.sh --broker-list localhost:9092 --topic artifacts
```

Archived and unpacked `apk`-s and metadata is in `$FETCHED_APKS_DIR` (`/tmp/fetched_apks` from example)

Flink UI is located here: `http://localhost:8081/`

If you want to see data in Kafka topics, you can run docker container inside cluster network and connect to the specific topic

`docker run --rm -it --network "artifacts_crawler_cluster" spotify/kafka /opt/kafka_2.11-0.10.1.0/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic artifacts --from-beginning`

## Configuration 

All configurations are in `./deploy` dir
