import time
import boto3
import sys

dynamodb = boto3.resource("dynamodb")

ENV = "uat"
TRANSACTIONS_TABLE_NAME = ENV+"-usrv-billing-core-transactions"
TRANSACTIONS_TABLE = dynamodb.Table(TRANSACTIONS_TABLE_NAME)
NAME_INDEX = "cycleIndex"
CYCLE = "2023-01-10"
COUNTRIES = [
    "Ecuador",
    "Colombia",
    "Peru",
    "Mexico",
    "Chile",
]

def get_merchants(country):
    items = []
    response = TRANSACTIONS_TABLE.query(
        ExpressionAttributeNames= {
            "#country": "country",
            "#calculateCycle": "calculateCycle"
        },
        ExpressionAttributeValues= {
            ":country": country,
            ":calculateCycle": CYCLE
        },
        IndexName= NAME_INDEX,
        FilterExpression= "#country = :country",
        KeyConditionExpression= "#calculateCycle = :calculateCycle"
    )
    items += response["Items"]
    while "LastEvaluatedKey" in response:
        response = TRANSACTIONS_TABLE.query(
            ExclusiveStartKey= response['LastEvaluatedKey'],
            ExpressionAttributeNames= {
                "#country": "country",
                "#calculateCycle": "calculateCycle"
            },
            ExpressionAttributeValues= {
                ":country": country,
                ":calculateCycle": CYCLE
            },
            IndexName= NAME_INDEX,
            FilterExpression= "#country = :country",
            KeyConditionExpression= "#calculateCycle = :calculateCycle"
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
                    'transactionId': item["transactionId"]
                },
                UpdateExpression="set #isOcb = :isOcb, #customerId = :customerId, #updatedAt = :updatedAt",
                ExpressionAttributeValues={
                    ':isOcb': False,
                    ':customerId': item['taxId'],
                    ':updatedAt': current_date_ts
                },
                ExpressionAttributeNames={
                    '#customerId': 'customerId',
                    '#isOcb': 'isOcb',
                    '#updatedAt': 'updatedAt',
                },
            )
        except Exception as e: 
            print(f"There was an error, transactionId: {item['transactionId']}, merchantId: {item['merchantId']}")

        
    print(f"\nchanged items: {changed}")



if __name__ == '__main__':
    for country in COUNTRIES:
        print(f"Country: {country}")
        merchants = get_merchants(country)
        update_items(merchants)
        time.sleep(2)