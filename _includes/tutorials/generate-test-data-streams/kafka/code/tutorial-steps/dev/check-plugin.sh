docker exec -i connect curl -s localhost:8083/connector-plugins|jq '.[].class'|grep Voluble
