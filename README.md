# Alert Lambda Function

Lambda function implementing an executor pattern to run scheduled tasks via CloudWatch Events/EventBridge rules. Each rule's name maps directly to its corresponding function, enabling independent scheduling.

## Architecture

- CloudWatch Events/EventBridge rules trigger the Lambda
- Rule name pattern: `<function_name>`
- Function mapping handled by Executor class
- Modular design for easy addition of new functions

## Setup

### Requirements
- AWS Lambda
- CloudWatch Events/EventBridge
- AWS Secrets Manager for credentials
- Python 3.10+

### Environment Variables
- `SECRET_ARN`: Secrets Manager ARN containing required credentials

## Implementation

### Adding New Functions
1. Add function to Executor class
2. Map function in `function_mapping` dictionary
3. Create corresponding CloudWatch rule which matches the function name and a schedule
4. Add rule as trigger to executor lambda function

## Included Functions

### goes_xrs_alert_stream
Get the latest GOES XRS data. Send the following kafka messages to [GCN](https://gcn.nasa.gov/)

* to the topic gcn.notices.swxsoc.goes_xrs_{severity}flare_alert where {} severity is one of [X10, X1, M5, M1], send a message if flux exceeds that threshold and when the flux decreases below the threshold
* to the topic gcn.notices.swxsoc.goes_xrs_flux_long send the latest goes xrs long flux value
* to the topic gcn.notices.swxsoc.goes_xrs_flux_short send the latest goes xrs short flux value

GOES XRS data can be approximately 4 minutes behind real-time.

## Error Handling
- HTTP 200: Successful execution
- HTTP 500: Execution failure with error details
- Comprehensive logging via swxsoc

