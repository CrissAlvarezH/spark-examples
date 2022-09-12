build:
	docker compose build

run-snowflake-connection: build
	export MODULE=snowflake-connection; docker compose up app

down:
	docker compose down