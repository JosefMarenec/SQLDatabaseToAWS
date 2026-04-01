# ─────────────────────────────────────────────────────────────
# Shared assume-role policy for all API Lambda functions
# ─────────────────────────────────────────────────────────────
data "aws_iam_policy_document" "lambda_assume" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "api_lambda" {
  name               = "${local.prefix}-api-lambda-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

# Basic Lambda execution (CloudWatch Logs)
resource "aws_iam_role_policy_attachment" "basic_execution" {
  role       = aws_iam_role.api_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# VPC access (ENI creation for private subnet access to RDS)
resource "aws_iam_role_policy_attachment" "vpc_access" {
  role       = aws_iam_role.api_lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaVPCAccessExecutionRole"
}

# Secrets Manager — read RDS credentials only
data "aws_iam_policy_document" "secrets_read" {
  statement {
    sid     = "ReadRDSSecret"
    effect  = "Allow"
    actions = ["secretsmanager:GetSecretValue"]
    resources = [
      "arn:aws:secretsmanager:${var.aws_region}:*:secret:${var.rds_secret_name}*"
    ]
  }
}

resource "aws_iam_policy" "secrets_read" {
  name   = "${local.prefix}-lambda-secrets-read"
  policy = data.aws_iam_policy_document.secrets_read.json
}

resource "aws_iam_role_policy_attachment" "secrets_read" {
  role       = aws_iam_role.api_lambda.name
  policy_arn = aws_iam_policy.secrets_read.arn
}
