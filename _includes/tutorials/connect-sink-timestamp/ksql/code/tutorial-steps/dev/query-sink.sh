 echo 'SELECT * FROM "TEMPERATURE_READINGS_TIMESTAMP_MT";' | docker exec -i postgres bash -c 'psql -U $POSTGRES_USER $POSTGRES_DB'
