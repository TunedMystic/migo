.PHONY: dev help venv

help: ## This help
	@echo "Usage:"
	@echo "  make <target>"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[1m%-15s\033[0m %s\n", $$1, $$2}'

venv:  ## Setup virtual environment
	@if [ ! -d "venv" ]; then \
		echo "making virtual env"; \
		python3 -m venv venv; \
	fi

install: venv  ## Install dev dependencies
	@( \
		. venv/bin/activate; \
		pip install --upgrade pip; \
		pip install -r requirements-dev.txt; \
	)

test:  ## Run tests
	@echo "Running tests"
	@( \
		. venv/bin/activate; \
		python -m unittest; \
	)
