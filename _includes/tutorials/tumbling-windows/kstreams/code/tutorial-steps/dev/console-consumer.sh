docker exec -it broker /usr/bin/kafka-console-consumer --topic rating-counts --bootstrap-server broker:9092 --from-beginning --property print.key=true
