set +m
function produce () { echo $1 | docker exec -i schema-registry /usr/bin/kafka-avro-console-producer --bootstrap-server broker:9092 --topic input-topic --property value.schema="$(< src/main/avro/pressure-alert.avsc)" & }
