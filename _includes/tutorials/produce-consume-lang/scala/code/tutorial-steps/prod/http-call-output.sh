curl http://localhost:8888/books | jq  '{results:[.results[].title] | sort_by(.)}'
