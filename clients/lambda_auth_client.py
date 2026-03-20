"""
Lambda Client Utility
Helper functions to invoke other AWS Lambda functions
"""

import json
import logging
import boto3
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)


class LambdaInvoker:
    """
    Client to invoke other AWS Lambda functions
    """
    
    def __init__(self):
        self.lambda_client = boto3.client('lambda')
    
    async def invoke_lambda(
        self,
        function_name: str,
        payload: Dict[str, Any],
        invocation_type: str = 'RequestResponse'
    ) -> Dict[str, Any]:
        """
        Invoke a Lambda function and return the response.
        
        Args:
            function_name: Name or ARN of the Lambda function
            payload: Dictionary payload to send to the Lambda
            invocation_type: 'RequestResponse' (sync) or 'Event' (async)
            
        Returns:
            Dictionary with the Lambda response
            
        Raises:
            Exception if Lambda invocation fails
        """
        try:
            logger.info(f"🚀 Invoking Lambda function: {function_name}")
            logger.debug(f"   Payload: {json.dumps(payload)}")
            
            response = self.lambda_client.invoke(
                FunctionName=function_name,
                InvocationType=invocation_type,
                Payload=json.dumps(payload)
            )
            
            # Read and parse response
            response_payload = json.loads(response['Payload'].read())
            
            logger.info(f"✅ Lambda invocation successful: {function_name}")
            logger.debug(f"   Response: {json.dumps(response_payload)}")
            
            return response_payload
            
        except Exception as e:
            logger.error(f"❌ Lambda invocation failed: {function_name} - {e}")
            raise
    
    async def get_access_token_from_lambda(
        self,
        token_service_lambda_name: str
    ) -> Optional[str]:
        """
        Get access token by calling the token service Lambda function.
        
        Your Lambda expects:
            {"cronjob_type": "get_access_token"}
        
        Your Lambda returns:
            {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": "{\"success\": true, \"access_token\": \"eyJ...\"}"
            }
        
        Args:
            token_service_lambda_name: Name of the Lambda function that returns tokens
            
        Returns:
            Access token string or None if failed
        """
        try:
            logger.info(f"🔐 Getting access token from Lambda: {token_service_lambda_name}")
            
            # Call the token service Lambda with YOUR specific payload format
            response = await self.invoke_lambda(
                function_name=token_service_lambda_name,
                payload={
                    'cronjob_type': 'get_access_token'  # Your Lambda's expected format
                }
            )
            
            logger.debug(f"   Raw response: {response}")
            
            # Parse YOUR Lambda's response format:
            # { "statusCode": 200, "body": "{\"success\": true, \"access_token\": \"...\"}" }
            
            if 'statusCode' in response:
                status_code = response['statusCode']
                
                if status_code == 200:
                    # Body is a JSON string, need to parse it
                    body_str = response.get('body', '{}')
                    
                    if isinstance(body_str, str):
                        try:
                            body = json.loads(body_str)
                        except json.JSONDecodeError as e:
                            logger.error(f"❌ Failed to parse body JSON: {e}")
                            logger.error(f"   Body content: {body_str}")
                            return None
                    else:
                        body = body_str
                    
                    # Check for success flag
                    success = body.get('success', False)
                    access_token = body.get('access_token')
                    
                    if success and access_token:
                        logger.info("✅ Access token retrieved successfully from Lambda")
                        logger.debug(f"   Token (first 50 chars): {access_token[:50]}...")
                        return access_token
                    elif not success:
                        logger.error(f"❌ Lambda returned success=false")
                        logger.error(f"   Response body: {body}")
                        return None
                    else:
                        logger.error("❌ No access_token in Lambda response body")
                        logger.error(f"   Response body: {body}")
                        return None
                else:
                    logger.error(f"❌ Lambda returned error status: {status_code}")
                    logger.error(f"   Response: {response}")
                    return None
            
            # Fallback: If Lambda returns { "access_token": "..." } directly (old format)
            elif 'access_token' in response:
                access_token = response['access_token']
                logger.info("✅ Access token retrieved successfully from Lambda (direct format)")
                return access_token
            
            else:
                logger.error(f"❌ Unexpected Lambda response format: {response}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Failed to get access token from Lambda: {e}", exc_info=True)
            return None


# Singleton instance
_lambda_invoker = None

def get_lambda_invoker() -> LambdaInvoker:
    """Get or create Lambda invoker singleton"""
    global _lambda_invoker
    if _lambda_invoker is None:
        _lambda_invoker = LambdaInvoker()
    return _lambda_invoker
