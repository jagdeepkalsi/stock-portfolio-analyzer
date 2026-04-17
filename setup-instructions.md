# AWS Portfolio Analyzer Deployment Guide

This guide will help you deploy your portfolio analyzer to AWS Lambda with daily automated execution.

## 🏗️ Architecture Overview

- **AWS Lambda** - Runs the portfolio analysis code
- **Amazon S3** - Stores your holdings.csv and portfolio.json files
- **AWS Secrets Manager** - Securely stores API keys and email credentials
- **Amazon EventBridge** - Triggers daily execution at 2 PM EST
- **CloudWatch** - Monitoring and logging

## 📋 Prerequisites

1. **AWS CLI** installed and configured
2. **Python 3.11+** installed
3. **Alpha Vantage API key** (free tier available)
4. **Gmail app password** for email notifications

## 🚀 Deployment Steps

### Step 1: Create Deployment S3 Bucket
```bash
aws s3 mb s3://portfolio-analyzer-deployment-YOUR-NAME
```

### Step 2: Update Configuration
Edit `deploy.sh` and update these variables:
```bash
S3_BUCKET="portfolio-analyzer-deployment-YOUR-NAME"
REGION="us-east-1"  # Or your preferred region
```

### Step 3: Deploy to AWS
```bash
./deploy.sh
```

### Step 4: Update Secrets in AWS Console

**API Keys Secret:**
1. Go to AWS Secrets Manager Console
2. Find `portfolio-analyzer/api-keys`
3. Update with your Alpha Vantage API key:
```json
{
  "alpha_vantage_api_key": "YOUR_ACTUAL_API_KEY"
}
```

**Email Config Secret:**
1. Find `portfolio-analyzer/email-config`
2. Update with your email credentials:
```json
{
  "smtp_user": "your-email@gmail.com",
  "smtp_password": "your-gmail-app-password"
}
```

### Step 5: Upload Portfolio Data

**Upload holdings.csv to S3:**
```bash
aws s3 cp holdings.csv s3://portfolio-analyzer-stack-portfolio-data/
```

**Upload portfolio.json to S3:**
```bash
aws s3 cp portfolio.json s3://portfolio-analyzer-stack-portfolio-data/
```

**Make sure your portfolio.json has email enabled:**
```json
{
    "settings": {
        "holdings_file": "holdings.csv",
        "send_email": true,
        "email_settings": {
            "recipient": "your-email@gmail.com",
            "smtp_server": "smtp.gmail.com",
            "smtp_port": 587
        }
    }
}
```

## 🧪 Testing

### Test the Lambda Function
```bash
aws lambda invoke --function-name portfolio-analyzer response.json
cat response.json
```

### View Logs
```bash
aws logs tail /aws/lambda/portfolio-analyzer --follow
```

## 📅 Scheduling

The function is automatically scheduled to run daily at **2 PM EST** using EventBridge.

**Note on Time Zones:**
- The CloudFormation template uses `cron(0 19 * * ? *)` (7 PM UTC)
- This equals 2 PM EST during standard time
- During daylight saving time (EDT), this will be 3 PM
- You may need to adjust the cron expression seasonally

### To Change Schedule:
1. Go to EventBridge Console
2. Find the rule named after your stack
3. Edit the cron expression:
   - `cron(0 18 * * ? *)` = 2 PM EDT / 1 PM EST
   - `cron(0 19 * * ? *)` = 3 PM EDT / 2 PM EST

## 📊 Monitoring

**CloudWatch Alarms are set up for:**
- Lambda function errors
- Function duration over 10 minutes

**View metrics in CloudWatch:**
- Function invocations
- Duration
- Error rate
- Memory usage

## 🔄 Updating Your Portfolio

### Update Holdings:
1. Edit your local `holdings.csv`
2. Upload to S3:
```bash
aws s3 cp holdings.csv s3://BUCKET-NAME/
```

### Update Code:
1. Make changes to `lambda_function.py`
2. Run deployment script:
```bash
./deploy.sh
```

## 💰 Cost Estimation

**Monthly costs (approximate):**
- Lambda: $0.20 (30 executions)
- S3: $0.05 (minimal storage)
- Secrets Manager: $0.40 (2 secrets)
- CloudWatch: $0.10 (logs and metrics)
- **Total: ~$0.75/month**

**Plus Alpha Vantage API costs if you exceed free tier**

## 🔧 Troubleshooting

### Common Issues:

1. **"No portfolio data available"**
   - Check S3 bucket has holdings.csv
   - Verify CSV format matches expected columns

2. **Email not sending**
   - Verify SMTP credentials in Secrets Manager
   - Check if send_email is true in portfolio.json
   - Verify Gmail app password (not regular password)

3. **Alpha Vantage API errors**
   - Check API key in Secrets Manager
   - Verify you haven't exceeded rate limits
   - Consider upgrading to premium API for reliability

4. **Lambda timeout**
   - Check CloudWatch logs for specific errors
   - Increase timeout in CloudFormation template if needed

### Useful Commands:

```bash
# Check CloudFormation stack status
aws cloudformation describe-stacks --stack-name portfolio-analyzer-stack

# View recent Lambda invocations
aws lambda list-provisioned-concurrency-configs --function-name portfolio-analyzer

# Download logs
aws logs download /aws/lambda/portfolio-analyzer

# Test with different event
aws lambda invoke --function-name portfolio-analyzer --payload '{}' response.json
```

## 🔐 Security Best Practices

- ✅ API keys stored in Secrets Manager
- ✅ S3 bucket blocks public access
- ✅ Lambda runs with minimal IAM permissions
- ✅ CloudWatch logs for audit trail
- ✅ Encrypted data at rest and in transit

## 📞 Support

If you encounter issues:
1. Check CloudWatch logs first
2. Verify all secrets are properly configured
3. Test Lambda function manually
4. Check S3 bucket permissions and contents

Happy investing! 📈