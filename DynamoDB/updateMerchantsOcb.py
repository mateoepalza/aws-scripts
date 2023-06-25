import time
import boto3
import pydash
import sys


from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
# Environment:
ENV = 'qa'

# Tabla de Billing Core Merchants:
merchant_table = ENV + '-usrv-billing-core-merchants'
webhook_merchant_table = dynamodb.Table(merchant_table)

# Tabla de Hierarchy Core:
hierarchy_table = ENV + '-usrv-hierarchy-core'
webhook_hierarchy_table = dynamodb.Table(hierarchy_table)


def update_data_merchant_3(element_hierarchy, specific_merchant):
    node_id = pydash.get(element_hierarchy, "nodeId")
    entity_name = pydash.get(element_hierarchy, "entityName")
    pydash.set_(specific_merchant, "nodeId", node_id)
    pydash.set_(specific_merchant, "entityName", entity_name)
    webhook_merchant_table.put_item(
        Item=specific_merchant,
    )


def update_data_merchant_2(specific_merchant):
    webhook_merchant_table.put_item(
        Item=specific_merchant,
    )


def get_current_date():
    return round(time.time() * 1000)


def get_merchants():
    merchants = scan_table(webhook_merchant_table, merchant_table, True)
    return merchants


def get_update_data_hierarchy(list):
    list_data_merchants = []
    merchant = scan_table(webhook_hierarchy_table, hierarchy_table)
    count = 0
    for item in list:
        count+=1
        pydash.set_(item, "nodeId", "N/A")
        pydash.set_(item, "entityName", "N/A")
        pydash.set_(item, "updatedAt", get_current_date())
        if pydash.get(item, "country", "null") != "null":
            pydash.set_(item, "constitutionalCountry", pydash.get(item, "country"))
        if count % 50 == 0:
            print(f" waiting in {count}...")
            time.sleep(2)
        update_data_merchant_2(item)
        list_data_merchants.append(pydash.get(item, "publicMerchantId"))

    count = 0
    for element in merchant:
        count+=1
        merchant_id = pydash.get(element, "merchantId")
        if count % 50 == 0:
            print(f" waiting in {count}...")
            time.sleep(2)
        if merchant_id in list_data_merchants:
            specific_merchant = get_specific_merchant(merchant_id, list)
            update_data_merchant_3(element, specific_merchant)


def get_specific_merchant(merchant_id, list):
    for index, element in enumerate(list):
        if element["publicMerchantId"] == merchant_id:
            return list[index]


def scan_table(webhook, table, filterByCountry = False):
    try:
        if filterByCountry:
            webhook.scan(FilterExpression = boto3.dynamodb.conditions.Attr("country").exists())
        else:
            webhook.scan()

    except ClientError as ce:
        if ce.response['Error']['Code'] == 'ResourceNotFoundException':
            print("TABLE: " + table + " NOT EXISTS, CREATE FIRST AND TRY AGAIN")
        sys.exit()

    if filterByCountry:
        response = webhook.scan( FilterExpression = boto3.dynamodb.conditions.Attr("country").exists())
    else: 
        response = webhook.scan()

    data = response['Items']
    while 'LastEvaluatedKey' in response:
        if filterByCountry: 
            response = webhook.scan(ExclusiveStartKey=response['LastEvaluatedKey'], FilterExpression = boto3.dynamodb.conditions.Attr("country").exists() )
        else: 
            response = webhook.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    return data


if __name__ == '__main__':
    data_merchants = get_merchants()
    if len(data_merchants) > 0:
        get_update_data_hierarchy(data_merchants)