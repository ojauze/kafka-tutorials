curl http://localhost:8080/books | jq --color-output '{results: [.results[].title]}'
