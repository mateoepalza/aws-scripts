import sys
import time
import boto3
ENV = "qa"
PROFILE = "Tungurahua"
REGION = "us-east-1"

def get_all_items(table):
    items = []
    response = table.scan()
    items += response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items += response['Items']
    return items
def update_items(table, items):
    count = 0
    changed = 0
    total_items = len(items)
    print(f"total items: {total_items}")
    current_date_ts = round(time.time() * 1000)
    for item in items:
        count += 1
        sys.stdout.write(f"\r{round(count * 100 / total_items)}%")
        if "activeDate" in item:
            continue
        if "created" not in item:
            continue
        changed += 1
        if count % 50 == 0:
            print(f" waiting in {count}...")
            time.sleep(2)
        table.update_item(
            Key={
                'publicMerchantId': item["publicMerchantId"]
            },
            UpdateExpression="set #activeDate=:ad, #updatedAt=:ua",
            ExpressionAttributeValues={
                ':ad': item["created"],
                ':ua': current_date_ts
            },
            ExpressionAttributeNames={
                '#activeDate': 'activeDate',
                '#updatedAt': 'updatedAt',
            },
        )
    print(f"\nchanged items: {changed}")
def set_missing_active_date(PROFILE: str, REGION: str, ENV: str):
    dynamo = boto3.Session(profile_name=PROFILE, region_name=REGION).resource("dynamodb")
    table = dynamo.Table(f"{ENV}-usrv-billing-core-merchants")
    items = get_all_items(table)
    update_items(table, items)
if __name__ == "__main__":
    set_missing_active_date(PROFILE, REGION, ENV)