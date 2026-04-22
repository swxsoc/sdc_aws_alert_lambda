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

- production: `sdc_aws_alert_lambda`
- development: `dev-sdc_aws_alert_lambda`
