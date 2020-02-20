.PHONY: help install test database remove

help: ## This help
	@echo "Usage:"
	@echo "  make <target>"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-15s\033[0m %s\n", $$1, $$2}'

install: venv  ## Install dev dependencies
	@echo "Installing dependencies"
	@poetry install

test:  ## Run tests
	@echo "Running tests"
	@poetry run python -m unittest

database:  ## Run docker database
# Make var at execution time
# Ref: https://stackoverflow.com/a/1909390
	$(eval DB_RUNNING := $(shell docker inspect -f '{{.State.Running}}' db 2>/dev/null))

ifneq ($(DB_RUNNING), true)
	@docker run -d --name db -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres postgres:11-alpine
endif

remove:  ## Remove docker database
	@docker container rm -fv db 2>/dev/null
