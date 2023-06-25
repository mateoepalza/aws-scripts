import time
import boto3
import sys

dynamodb = boto3.resource("dynamodb")

ENV = "uat"
TABLE_NAME = ENV+"-usrv-billing-core-merchants"
aws_session = boto3.session.Session(region_name="us-east-1", profile_name="Dev")
aws_dynamodb = aws_session.resource('dynamodb')
TRANSACTIONS_TABLE = aws_dynamodb.Table(TABLE_NAME)
NAME_INDEX = "countryIndex"
COUNTRIES = [
    #"Ecuador",
    #"Colombia",
    #"Peru",
    "Mexico",
    #"Chile",
]

def get_merchants(country):
    items = []
    response = TRANSACTIONS_TABLE.query(
        ExpressionAttributeNames= {
            "#country": "country",
            "#entityName": "entityName"
        },
        ExpressionAttributeValues= {
            ":country": country,
            ":entityName": "BRANCH"
        },
        IndexName= NAME_INDEX,
        FilterExpression= "#entityName = :entityName",
        KeyConditionExpression= "#country = :country"
    )
    items += response["Items"]
    while "LastEvaluatedKey" in response:
        response = TRANSACTIONS_TABLE.query(
            ExclusiveStartKey= response['LastEvaluatedKey'],
             ExpressionAttributeNames= {
            "#country": "country",
            "#entityName": "entityName"
        },
        ExpressionAttributeValues= {
            ":country": country,
            ":entityName": "N/A"
        },
        IndexName= NAME_INDEX,
        FilterExpression= "#entityName = :entityName",
        KeyConditionExpression= "#country = :country"
        )
        items += response['Items']
    return items

def update_items(items):
    count = 0
    changed = 0
    total_items = len(items)
    print(f"total items: {total_items}")
    current_date_ts = round(time.time() * 1000)
    for item in items:
        try:
            count += 1
            sys.stdout.write(f"\r{round(count * 100 / total_items)}%")
            changed += 1
            if count % 50 == 0:
                print(f" waiting in {count}...")
                time.sleep(2)
            TRANSACTIONS_TABLE.update_item(
                Key={
                    'publicMerchantId': item["publicMerchantId"]
                },
                UpdateExpression="set #entityName = :entityName, #updatedAt = :updatedAt",
                ExpressionAttributeValues={
                    ':entityName': "BRANCHES",
                    ':updatedAt': current_date_ts
                },
                ExpressionAttributeNames={
                    '#entityName': 'entityName',
                    '#updatedAt': 'updatedAt',
                },
            )
        except Exception as e: 
            print(f"There was an error, transactionId: {item['publicMerchantId']}, merchantId: {item['publicMerchantId']}")

        
    print(f"\nchanged items: {changed}")



if __name__ == '__main__':
    for country in COUNTRIES:
        print(f"Country: {country}")
        merchants = get_merchants(country)
        print(len(merchants))
        #update_items(merchants)
        time.sleep(2)