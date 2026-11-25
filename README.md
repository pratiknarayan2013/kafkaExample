# kafkaExample
./kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092
./kafka-console-producer.sh --topic test-topic --bootstrap-server localhost:9092
./kafka-console-consumer.sh --topic test-topic --from-beginning --bootstrap-server localhost:9092