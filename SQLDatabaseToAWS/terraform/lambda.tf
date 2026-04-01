# ─────────────────────────────────────────────────────────────
# Package the api/ directory into a zip for Lambda deployment
# ─────────────────────────────────────────────────────────────
data "archive_file" "api_zip" {
  type        = "zip"
  source_dir  = local.lambda_src
  output_path = local.zip_output
  excludes = [
    ".git",
    ".claude",
    "terraform",
    "tests",
    "__pycache__",
    "*.pyc",
    "*.pyo",
    ".pytest_cache",
    "node_modules",
  ]
}

# ─────────────────────────────────────────────────────────────
# Shared environment variables for all API Lambdas
# ─────────────────────────────────────────────────────────────
locals {
  lambda_env = {
    RDS_SECRET_NAME    = var.rds_secret_name
    RDS_HOST           = var.rds_host
    RDS_PORT           = tostring(var.rds_port)
    DB_NAME            = var.db_name
    ENVIRONMENT        = var.environment
    LOG_LEVEL          = var.log_level
    AWS_DEFAULT_REGION = var.aws_region
  }
}

# ─────────────────────────────────────────────────────────────
# CloudWatch Log Groups (explicit, so retention is managed)
# ─────────────────────────────────────────────────────────────
resource "aws_cloudwatch_log_group" "health" {
  name              = "/aws/lambda/${local.prefix}-health"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "products" {
  name              = "/aws/lambda/${local.prefix}-products"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "customers" {
  name              = "/aws/lambda/${local.prefix}-customers"
  retention_in_days = 14
}

resource "aws_cloudwatch_log_group" "orders" {
  name              = "/aws/lambda/${local.prefix}-orders"
  retention_in_days = 14
}

# ─────────────────────────────────────────────────────────────
# Lambda: health
# ─────────────────────────────────────────────────────────────
resource "aws_lambda_function" "health" {
  function_name    = "${local.prefix}-health"
  description      = "GET /health — DB connectivity check"
  role             = aws_iam_role.api_lambda.arn
  runtime          = var.lambda_runtime
  handler          = "api.handlers.health.lambda_handler"
  filename         = data.archive_file.api_zip.output_path
  source_code_hash = data.archive_file.api_zip.output_base64sha256
  memory_size      = var.lambda_memory_mb
  timeout          = var.lambda_timeout_s

  vpc_config {
    subnet_ids         = var.vpc_subnet_ids
    security_group_ids = var.vpc_security_group_ids
  }

  environment {
    variables = local.lambda_env
  }

  depends_on = [aws_cloudwatch_log_group.health]
}

# ─────────────────────────────────────────────────────────────
# Lambda: products
# ─────────────────────────────────────────────────────────────
resource "aws_lambda_function" "products" {
  function_name    = "${local.prefix}-products"
  description      = "CRUD /products"
  role             = aws_iam_role.api_lambda.arn
  runtime          = var.lambda_runtime
  handler          = "api.handlers.products.lambda_handler"
  filename         = data.archive_file.api_zip.output_path
  source_code_hash = data.archive_file.api_zip.output_base64sha256
  memory_size      = var.lambda_memory_mb
  timeout          = var.lambda_timeout_s

  vpc_config {
    subnet_ids         = var.vpc_subnet_ids
    security_group_ids = var.vpc_security_group_ids
  }

  environment {
    variables = local.lambda_env
  }

  depends_on = [aws_cloudwatch_log_group.products]
}

# ─────────────────────────────────────────────────────────────
# Lambda: customers
# ─────────────────────────────────────────────────────────────
resource "aws_lambda_function" "customers" {
  function_name    = "${local.prefix}-customers"
  description      = "CRUD /customers"
  role             = aws_iam_role.api_lambda.arn
  runtime          = var.lambda_runtime
  handler          = "api.handlers.customers.lambda_handler"
  filename         = data.archive_file.api_zip.output_path
  source_code_hash = data.archive_file.api_zip.output_base64sha256
  memory_size      = var.lambda_memory_mb
  timeout          = var.lambda_timeout_s

  vpc_config {
    subnet_ids         = var.vpc_subnet_ids
    security_group_ids = var.vpc_security_group_ids
  }

  environment {
    variables = local.lambda_env
  }

  depends_on = [aws_cloudwatch_log_group.customers]
}

# ─────────────────────────────────────────────────────────────
# Lambda: orders
# ─────────────────────────────────────────────────────────────
resource "aws_lambda_function" "orders" {
  function_name    = "${local.prefix}-orders"
  description      = "CRUD /orders + /customers/{id}/orders"
  role             = aws_iam_role.api_lambda.arn
  runtime          = var.lambda_runtime
  handler          = "api.handlers.orders.lambda_handler"
  filename         = data.archive_file.api_zip.output_path
  source_code_hash = data.archive_file.api_zip.output_base64sha256
  memory_size      = var.lambda_memory_mb
  timeout          = var.lambda_timeout_s

  vpc_config {
    subnet_ids         = var.vpc_subnet_ids
    security_group_ids = var.vpc_security_group_ids
  }

  environment {
    variables = local.lambda_env
  }

  depends_on = [aws_cloudwatch_log_group.orders]
}
