import boto3
import json
from datetime import datetime

def lambda_handler(event, context):
    """
    L0: Start Step Function when user clicks Sync
    """
    stepfunctions = boto3.client('stepfunctions')
    
    try:
        # Parse POST request body
        body = json.loads(event.get('body', '{}'))
        
        # Validate required inputs
        account_id = body['account_id']
        location_id = body['location_id']
        api_base_url = body['api_base_url']
        
        # Generate unique execution name
        timestamp = int(datetime.now().timestamp())
        execution_name = f"revi-mt-data-sync-{account_id}-{location_id}-{timestamp}"
        

        # Start Step Function
        response = stepfunctions.start_execution(
            stateMachineArn='arn:aws:states:us-east-1:491085429701:stateMachine:revi-crm-data-sync-pipelines',
            name=execution_name,
            input=json.dumps({
                'account_id': account_id,
                'location_id': location_id,
                'api_base_url': api_base_url
            })
        )
        
        return {
            'statusCode': 200,
            'headers': {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'POST'
            },
            'body': json.dumps({
                'message': 'Sync started successfully',
                'executionArn': response['executionArn'],
                'executionName': execution_name,
                'status': 'STARTED'
            })
        }
        
    except KeyError as e:
        return {
            'statusCode': 400,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                'error': f'Missing required field: {str(e)}'
            })
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                'error': f'Failed to start sync: {str(e)}'
            })
        }
