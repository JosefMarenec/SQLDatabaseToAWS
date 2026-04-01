output "api_base_url" {
  description = "Base URL of the deployed API"
  value       = "https://${aws_api_gateway_rest_api.main.id}.execute-api.${var.aws_region}.amazonaws.com/${var.environment}"
}

output "api_gateway_id" {
  description = "API Gateway REST API ID"
  value       = aws_api_gateway_rest_api.main.id
}

output "lambda_function_arns" {
  description = "ARNs of all deployed Lambda functions"
  value = {
    health    = aws_lambda_function.health.arn
    products  = aws_lambda_function.products.arn
    customers = aws_lambda_function.customers.arn
    orders    = aws_lambda_function.orders.arn
  }
}

output "iam_role_arn" {
  description = "IAM role ARN shared by all API Lambdas"
  value       = aws_iam_role.api_lambda.arn
}
