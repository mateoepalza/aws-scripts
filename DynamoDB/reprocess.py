import boto3
import time

# environment
ENV = "uat"
# Create DynamoDB client
dynamodb = boto3.client('dynamodb')
# Table name
table_name = f'{ENV}-usrv-billing-transactions'
# Query
query = {
    'FilterExpression': 'transactionType <> :void_manual',
    'ExpressionAttributeValues': {
        ':void_manual': {'S': 'VOID_MANUAL'}
    }
}

print(table_name)
# Function to delete the data
def delete_data():
    print("Eliminando datos...")
    response = dynamodb.scan(TableName=table_name, **query)
    items = response['Items']
    count = 0  # contador de items eliminados desde la última pausa
    total_count = 0  # contador de items eliminados en total
    for item in items:
        dynamodb.delete_item(TableName=table_name, Key={'id': {'S': item['id']['S']}})
        count += 1  # incrementar contador
        total_count += 1
        if count % 1000 == 0:  # cada 1000 items eliminados desde la última pausa
            print(f"Haciendo una pausa de 2 segundos... Se eliminaron {total_count}")
            time.sleep(2)
            count = 0
    print(f"Se eliminaron {total_count} items de la tabla {table_name}.")
if __name__ == '__main__':
    delete_data()