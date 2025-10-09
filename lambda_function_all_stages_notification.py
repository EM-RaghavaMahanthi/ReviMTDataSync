import json
import urllib3
import os
from datetime import datetime, timezone

# Initialize HTTP client
http = urllib3.PoolManager()

def lambda_handler(event, context):
    """
    L4: Notification Hub - Receives status from Step Function and sends to frontend
    """
    
    try:
        print(f"Received event: {json.dumps(event)}")
        
        # Parse status from Step Function
        stage = event.get('stage', 'unknown')          # 'stage1', 'stage2', 'stage3'
        status = event.get('status', 'unknown')        # 'SUCCESS' or 'FAILED'
        message = event.get('message', 'No message')   # Human readable message
        execution_id = event.get('executionId', 'unknown')
        
        # Create standardized notification payload
        notification = {
            'executionId': execution_id,
            'stage': stage,
            'status': status,
            'message': message,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'progress': get_progress_percentage(stage, status)
        }
        
        print(f"Sending notification: {json.dumps(notification)}")
        
        # For now, just log the notification (frontend team will implement webhook later)
        webhook_url = os.environ.get('FRONTEND_WEBHOOK_URL', '')
        
        if webhook_url:
            # Send to frontend webhook
            result = send_webhook_notification(notification, webhook_url)
        else:
            # Just log for now - frontend team will configure webhook URL later
            print(f"📢 NOTIFICATION: {notification}")
            result = {'method': 'logged', 'success': True}
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Notification processed successfully',
                'notification': notification,
                'method': result.get('method', 'logged')
            })
        }
        
    except Exception as e:
        print(f"L4 Notification error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def get_progress_percentage(stage, status):
    """Calculate progress percentage based on stage completion"""
    if status == 'FAILED':
        return None
        
    progress_map = {
        'stage1': 33,   # CRM to S3 complete (33%)
        'stage2': 66,   # DB sync complete (66%)
        'stage3': 100   # All complete (100%)
    }
    return progress_map.get(stage, 0)

def send_webhook_notification(notification, webhook_url):
    """Send HTTP POST to frontend webhook"""
    try:
        webhook_token = os.environ.get('WEBHOOK_TOKEN', '')
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        if webhook_token:
            headers['Authorization'] = f'Bearer {webhook_token}'
        
        encoded_data = json.dumps(notification).encode('utf-8')
        
        response = http.request(
            'POST',
            webhook_url,
            body=encoded_data,
            headers=headers,
            timeout=10.0
        )
        
        print(f"Webhook response status: {response.status}")
        return {'method': 'webhook', 'success': response.status == 200}
        
    except Exception as e:
        print(f"Webhook failed: {str(e)}")
        return {'method': 'webhook', 'success': False, 'error': str(e)}
