SHELL := /bin/bash

.PHONY: help install test database remove clean

help:  ## This help
	@echo "Usage:"
	@echo "  make <target>"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-15s\033[0m %s\n", $$1, $$2}'

install:  ## Install dev dependencies
	@echo "Installing dependencies"
	@poetry install

test:  ## Run tests
	@echo "Running tests"
	@poetry run coverage run --source '.' -m unittest
	@poetry run coverage report

database:  ## Run docker database
# Make var at execution time
# Ref: https://stackoverflow.com/a/1909390
	@DB_RUNNING=$$(docker inspect -f '{{.State.Running}}' db 2>/dev/null); \
	if [ "$$DB_RUNNING" = "true" ]; then \
		echo "DB container is already running"; \
	else \
		docker run -d --name db -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres postgres:11-alpine; \
	fi

remove:  ## Remove docker database
	@docker container rm -fv db 2>/dev/null

clean:  ## Clean workspace
	@echo "Cleaning workspace"
	@find . -type f -name "*.pyc" -delete
	@find . -type d -name "__pycache__" -delete
	@find . -type f -name "*.DS_Store" -delete
	@rm -f .coverage
	@rm -rf *.egg-info
	@rm -rf dist

publish-test:  ## Publish to test PyPI
# To publish to test pypi, you have to configure the repository url in poetry first.
#
# poetry config repositories.testpypi https://test.pypi.org/legacy/
# poetry publish -r testpypi --username USERNAME --password PASSWORD --build
	@echo -e "Publishing to PyPI test\n"
	@read -p "Username: " username; \
	read -p "Password: " password; \
	echo "poetry publish -r https://test.pypi.org/legacy/ --username $$username --password $$password --build"

publish:  ## Publish to PyPI
	@echo -e "Publishing to PyPI\n"
	@read -p "Username: " username; \
	read -p "Password: " password; \
	echo "poetry publish --username $$username --password $$password --build"
