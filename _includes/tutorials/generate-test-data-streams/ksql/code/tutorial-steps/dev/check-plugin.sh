docker exec -i ksqldb curl -s localhost:8083/connector-plugins|jq '.[].class'
