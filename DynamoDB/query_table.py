import boto3

dynamodb = boto3.resource("dynamodb")

def queryTable(tableName, indexName, query):
    response = dynamodb.Table(tableName).query(
        IndexName = indexName,
        ExpressionAttributeValues = query['expressionAttributeValues'],
        ExpressionAttributeNames = query['expressionAttributeNames'],
        FilterExpression = query["filterExpression"],
        KeyConditionExpression = query["KeyConditionExpression"]
    )

    print("Total objects found: ", response["Count"])

    for item in response['Items']:
        print(item)

def build_query(country, age):
    query= {
        "expressionAttributeNames": {
            "#country": "country",
            "#age": "age"
        },
        "expressionAttributeValues": {
            ":country": country,
            ":age": age
        },
        "filterExpression" : "#age = :age",
        "KeyConditionExpression": "#country = :country"
    }

    return query

if __name__ == "__main__":
    tableName = "clients"
    indexName = "countryIndex"
    country = "Colombia"
    age = 24
    queryTable(tableName, indexName, build_query(country, age))