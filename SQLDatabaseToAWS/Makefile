.PHONY: install test lint format build deploy-dev deploy-prod migrate clean

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────
PYTHON      := python3
PIP         := pip3
TF_DIR      := terraform
ENV         ?= dev

# ─────────────────────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────────────────────
install:
	$(PIP) install -r requirements.txt
	$(PIP) install pytest pytest-cov ruff

# ─────────────────────────────────────────────────────────────
# Testing
# ─────────────────────────────────────────────────────────────
test:
	$(PYTHON) -m pytest tests/ -v --tb=short

test-cov:
	$(PYTHON) -m pytest tests/ -v --cov=api --cov-report=term-missing --cov-report=html

# ─────────────────────────────────────────────────────────────
# Linting & Formatting
# ─────────────────────────────────────────────────────────────
lint:
	ruff check api/ tests/

format:
	ruff format api/ tests/

# ─────────────────────────────────────────────────────────────
# Build (zip for Lambda)
# ─────────────────────────────────────────────────────────────
build:
	mkdir -p $(TF_DIR)/.build
	zip -r $(TF_DIR)/.build/lambda.zip api/ migrations/ \
		-x "*/__pycache__/*" -x "*.pyc" -x "*.pyo"
	@echo "Build complete: $(TF_DIR)/.build/lambda.zip"

# ─────────────────────────────────────────────────────────────
# Terraform / Deploy
# ─────────────────────────────────────────────────────────────
tf-init:
	cd $(TF_DIR) && terraform init

tf-plan:
	cd $(TF_DIR) && terraform plan -var="environment=$(ENV)"

tf-apply:
	cd $(TF_DIR) && terraform apply -var="environment=$(ENV)" -auto-approve

deploy-dev: build
	$(MAKE) tf-apply ENV=dev

deploy-prod: test build
	$(MAKE) tf-apply ENV=prod

tf-destroy:
	cd $(TF_DIR) && terraform destroy -var="environment=$(ENV)"

# ─────────────────────────────────────────────────────────────
# DB Migrations (requires psql in PATH)
# ─────────────────────────────────────────────────────────────
migrate:
	@echo "Running migrations against $$RDS_HOST/$$DB_NAME"
	for f in migrations/V*.sql; do \
		echo "Applying $$f ..."; \
		psql "host=$$RDS_HOST port=$${RDS_PORT:-5432} dbname=$$DB_NAME user=$$DB_USER" -f $$f; \
	done

# ─────────────────────────────────────────────────────────────
# Utilities
# ─────────────────────────────────────────────────────────────
clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	rm -rf $(TF_DIR)/.build .pytest_cache htmlcov .coverage
