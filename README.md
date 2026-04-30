# SWxSOC Alert Lambda

Lambda container for GOES XRS alert generation and GCN Kafka publication.

## Purpose

This repository packages a focused alerting Lambda that:

- runs from EventBridge / CloudWatch scheduled rules
- fetches recent GOES XRS flux data from NOAA
- publishes the latest flux stream to GCN Kafka
- emits flare threshold crossing alerts when flux rises above or falls below configured severities

## Runtime Inputs

The Lambda expects EventBridge events with a rule name that maps to a function in the alert dispatcher.

Required environment variables:

- `GCN_CLIENT_ID_SECRET_ARN`: Secrets Manager ARN containing `gcn_client_id`
- `GCN_CLIENT_SECRET_SECRET_ARN`: Secrets Manager ARN containing `gcn_client_secret`
- `GCN_DOMAIN`: Optional Kafka domain override. Defaults to `test.gcn.nasa.gov`

## Local Validation

Install dependencies:

```bash
pip install -r requirements.txt
```

Run tests:

```bash
pytest
```

## Deployment Notes

This repository builds the Lambda container image as a standalone ancillary asset.

It does not need to be represented in the local Terraform repo.

If you publish it through CodeBuild, the current buildspec pushes to:

- `swxsoc_sdc_aws_alert_lambda`

The CodeBuild service role must allow ECR authentication and image push
operations for those repositories. Attach a policy like this to the build role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "EcrLogin",
      "Effect": "Allow",
      "Action": "ecr:GetAuthorizationToken",
      "Resource": "*"
    },
    {
      "Sid": "PushAlertLambdaImages",
      "Effect": "Allow",
      "Action": [
        "ecr:BatchCheckLayerAvailability",
        "ecr:CompleteLayerUpload",
        "ecr:DescribeRepositories",
        "ecr:InitiateLayerUpload",
        "ecr:PutImage",
        "ecr:UploadLayerPart"
      ],
      "Resource": [
        "arn:aws:ecr:us-east-1:351967858401:repository/swxsoc_sdc_aws_alert_lambda"
      ]
    }
  ]
}
```
