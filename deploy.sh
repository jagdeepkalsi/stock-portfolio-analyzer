#!/bin/bash

# AWS Lambda Deployment Script for Portfolio Analyzer
set -e

echo "🚀 Starting deployment of Portfolio Analyzer to AWS Lambda..."

# Configuration
FUNCTION_NAME="portfolio-analyzer"
REGION="us-west-2"
S3_BUCKET="portfolio-analyzer-deployment-jagdeep"
STACK_NAME="portfolio-analyzer-stack"

# Create deployment package
echo "📦 Creating deployment package..."
rm -rf build/
mkdir -p build/

# Copy Lambda function and supporting modules
cp lambda_function.py build/
cp data_providers.py build/
cp news_providers.py build/
cp news_scorer.py build/
cp news_alert.py build/

# Install dependencies
echo "📚 Installing Python dependencies..."
pip3 install -r requirements-lambda.txt -t build/

# Create deployment zip
echo "🗜️ Creating deployment zip..."
cd build
zip -r ../lambda-deployment.zip .
cd ..

# Upload to S3
echo "☁️ Uploading to S3..."
aws s3 cp lambda-deployment.zip s3://$S3_BUCKET/

# Deploy CloudFormation stack
echo "🏗️ Deploying CloudFormation stack..."
aws cloudformation deploy \
    --template-file cloudformation-template.yaml \
    --stack-name $STACK_NAME \
    --parameter-overrides \
        S3Bucket=$S3_BUCKET \
        S3Key=lambda-deployment.zip \
    --capabilities CAPABILITY_IAM \
    --region $REGION

# Update Lambda function code (portfolio analyzer)
echo "🔄 Updating Lambda function code..."
aws lambda update-function-code \
    --function-name $FUNCTION_NAME \
    --s3-bucket $S3_BUCKET \
    --s3-key lambda-deployment.zip \
    --region $REGION

# Update news alert Lambda function code
echo "🔄 Updating news alert Lambda function code..."
aws lambda update-function-code \
    --function-name portfolio-news-alert \
    --s3-bucket $S3_BUCKET \
    --s3-key lambda-deployment.zip \
    --region $REGION

echo "✅ Deployment completed successfully!"
echo ""
echo "📋 Next steps:"
echo "1. Update the secrets in AWS Secrets Manager:"
echo "   - portfolio-analyzer/api-keys  (add finnhub_api_key)"
echo "   - portfolio-analyzer/email-config"
echo "2. Upload your holdings.csv and portfolio.json to S3 bucket"
echo "3. Enable news alerts in portfolio.json: settings.news_alerts.enabled = true"
echo "4. Test both Lambda functions"
echo ""
echo "🔗 Useful commands:"
echo "aws lambda invoke --function-name $FUNCTION_NAME response.json && cat response.json"
echo "aws lambda invoke --function-name portfolio-news-alert news-response.json && cat news-response.json"
echo "aws logs tail /aws/lambda/$FUNCTION_NAME --follow"
echo "aws logs tail /aws/lambda/portfolio-news-alert --follow"