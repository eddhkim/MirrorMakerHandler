# MirrorMakerHandler
Simple custom mirror maker message handler to replicate messages to different topic names.

## Setup
Install `kafka-mirror-maker.sh` from [Kafka](http://kafka.apache.org/downloads.html) first

```
mvn clean package
export CLASSPATH=$(pwd)/target/mirror-maker-handler-1.0.0.jar
```


## Run

```
bin/kafka-mirror-maker.sh --consumer.config config/consumer.properties --producer.config config/producer.properties --message.handler org.apache.kafka.custom.ReplicationMessageHandler  --whitelist TEST
```
