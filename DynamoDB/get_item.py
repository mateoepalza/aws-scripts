import boto3

dynamodb = boto3.resource("dynamodb")

ENV = "uat"
TRANSACTIONS_TABLE_NAME = ENV+"-usrv-billing-core-merchants"
TRANSACTIONS_TABLE = dynamodb.Table(TRANSACTIONS_TABLE_NAME)
NAME_INDEX = "countryIndex"
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
        ExpressionAttributeNames={
            "#country": "country",
            "#entityName": "entityName",
            "#minAmount": "minAmount",
            "#type": "type",  # Added an alias for "type" attribute
        },
        ExpressionAttributeValues={
            ":country": country,
            ":entityName1": "BRANCH",
            ":entityName2": "CUSTOMER",
            ":type1": "deductible",
            ":type2": "fixed",
            ":type3": "notCharge",
        },
        IndexName=NAME_INDEX,
        FilterExpression="attribute_exists(#minAmount) AND (#entityName = :entityName1 OR #entityName = :entityName2) AND (#minAmount.#type <> :type1 AND #minAmount.#type <> :type2 AND #minAmount.#type <> :type3)",  # Replaced "#minAmount.type" with "#type"
        KeyConditionExpression="#country = :country"
    )
    items += response["Items"]
    while "LastEvaluatedKey" in response:
        response = TRANSACTIONS_TABLE.query(
            ExclusiveStartKey=response['LastEvaluatedKey'],
            ExpressionAttributeNames={
                "#country": "country",
                "#entityName": "entityName",
                "#minAmount": "minAmount",
                "#type": "type"  # Added an alias for "type" attribute
            },
            ExpressionAttributeValues={
                ":country": country,
                ":entityName1": "BRANCH",
                ":entityName2": "CUSTOMER",
                ":type1": "deductible",
                ":type2": "fixed",
                ":type3": "notCharge",
            },
            IndexName=NAME_INDEX,
            FilterExpression="attribute_exists(#minAmount) AND (#entityName = :entityName1 OR #entityName = :entityName2) AND (#minAmount.#type <> :type1 AND #minAmount.#type <> :type2 AND #minAmount.#type <> :type3)",  # Replaced "#minAmount.type" with "#type"
            KeyConditionExpression="#country = :country"
        )
        items += response['Items']
    return items


if __name__ == "__main__":
   items =  get_merchants("Colombia")
   print(items)