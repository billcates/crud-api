import json
import boto3
from decimal import Decimal



dynamodbTableName='product_table'
dynamodb= boto3.resource('dynamodb')
table=dynamodb.Table(dynamodbTableName)

getMethod='GET'
postMethod='POST'
putMethod='PUT'
deleteMethod='DELETE'

healthPath='/health'
productPath='/product'
productsPath='/products'

class CustomEncoder(json.JSONEncoder):
    def default(self,obj):
        if isinstance(obj,Decimal):
            return float(obj)
        return json.JSONEncoder.default(self,obj)

def buildResponse(statusCode, body=None):
    response={
        'statusCode': statusCode,
        'headers':{
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
    }
    if body is not None:
        response['body']=json.dumps(body,cls=CustomEncoder)
    return response

def getProduct(productId):
    response=table.get_item(
        Key={
            'product_id':productId
        })
    if 'Item' in response:
        return buildResponse(200,response['Item'])
    
    else:
        return buildResponse(404,{'Message':'ProductId : %s not found' % productId })
        
def getProducts():
    response=table.scan()
    result=response['Items']
    
    while 'LastEvaluatedKey' in response:
        response=table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        result.extend(response['Items'])
        
    body={
        'products':result
    }
    
    return buildResponse(200,body)
    
def saveProduct(requestBody):
    table.put_item(Item=requestBody)
    body = {
        'Operation': 'Add',
        'Message': 'Success',
        'Item': requestBody
    }
    return buildResponse(200, body)

    
def updateProduct(productId, queryParams):
    response = table.get_item(
        Key={
            'product_id': productId
        }
    )
    if 'Item' in response:
        item = response['Item']

        # Prepare the update expression and attribute values
        updateExpression = 'SET '
        expressionAttributeValues = {}
        for key, value in queryParams.items():
            updateExpression += f' {key} = :{key},'
            expressionAttributeValues[f':{key}'] = value
        updateExpression = updateExpression[:-1]  # Remove the trailing comma

        # Perform the update
        response = table.update_item(
            Key={
                'product_id': productId
            },
            UpdateExpression=updateExpression,
            ExpressionAttributeValues=expressionAttributeValues,
            ReturnValues='ALL_NEW'
        )

        body = {
            'Operation': 'update',
            'Message': 'Success',
            'UpdatedAttributes': response['Attributes']
        }
        return buildResponse(200, body)
    else:
        return buildResponse(404, {'Message': 'ProductId: %s not found' % productId})

    
def deleteProduct(productId):
    response = table.delete_item(
        Key={
            'product_id': productId
        },
        ReturnValues='ALL_OLD'
    )
    body = {
        'Operation': 'Delete',
        'Message': 'Success',
        'deletedItem': response 
    }
    return buildResponse(200, body)


def lambda_handler(event, context):
    # TODO implement
    print(event)
    httpMethod=event['httpMethod']
    path=event['path']
    
    if httpMethod==getMethod and path==healthPath:
        response=buildResponse(200)
    
    elif httpMethod==getMethod and path==productPath:
        response=getProduct(event["queryStringParameters"]["product_id"])
        
    elif httpMethod==getMethod and path==productsPath:
        response=getProducts()
    
    elif httpMethod==postMethod and path==productPath:
        response=saveProduct(json.loads(event["body"]))
        
    elif httpMethod==putMethod and path==productPath:
        #param=json.loads(event["body"])
        #response=updateProduct(param["product_id"],param["updateKey"],param["updateValue"])
        productId = event['queryStringParameters']['product_id']
        queryParams = event['queryStringParameters']
        queryParams.pop('product_id', None)
        response = updateProduct(productId, queryParams)
    
    elif httpMethod==deleteMethod and path==productPath:
        param=json.loads(event["body"])
        response=deleteProduct(param["product_id"])
    
    else:
        response=buildResponse(404,'Not Found')
        
    return response