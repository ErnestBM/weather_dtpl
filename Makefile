PRODUCE_COMPOSE_PATH := ./weather_source/docker-compose.yml

produce-up:
	docker compose -f $(PRODUCE_COMPOSE_PATH) up --build -d

produce-down:
	docker compose -f $(PRODUCE_COMPOSE_PATH) down

produce-restart: 
	stream-down 
	stream-up
