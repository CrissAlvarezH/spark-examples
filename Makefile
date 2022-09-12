build:
	docker compose build

run: build
	docker compose up app

down:
	docker compose down