.PHONY: build test stress run docker-up docker-down harness

build:
	./gradlew build

test:
	./gradlew check

stress:
	./gradlew :core:stressTest

run:
	./gradlew :server:run

# Service on :8080 and Prometheus on :9090.
docker-up:
	docker compose up -d --build

docker-down:
	docker compose down

# Load test against the compose service (run `make docker-up` first).
harness:
	docker compose --profile harness run --rm harness
