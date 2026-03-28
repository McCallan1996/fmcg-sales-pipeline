.PHONY: help setup infra-up infra-down docker-up docker-down

help:
	@echo "Available targets:"
	@echo "  setup       - copy .env.example to .env (edit it after)"
	@echo "  infra-up    - provision GCP resources with terraform"
	@echo "  infra-down  - tear down GCP resources"
	@echo "  docker-up   - start all containers"
	@echo "  docker-down - stop and remove containers"

setup:
	cp .env.example .env
	@echo "Now edit .env with your actual values"

infra-up:
	cd terraform && terraform init && terraform apply -auto-approve

infra-down:
	cd terraform && terraform destroy -auto-approve

docker-up:
	cd docker && docker-compose --env-file ../.env up -d

docker-down:
	cd docker && docker-compose down -v
