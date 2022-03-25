import boto3
dynamodb = boto3.resource("dynamodb")

def create_table():
    table = dynamodb.create_table(
        TableName = "clients",
        AttributeDefinitions=[
            {
                "AttributeName": "id",
                "AttributeType": "S"
            },
            {
                "AttributeName": "country",
                "AttributeType": "S"
            }
        ],
        KeySchema = [
            {
                "AttributeName": "id",
                "KeyType": "HASH"
            }
        ],
        GlobalSecondaryIndexes= [
            {
                "IndexName": "countryIndex",
                "KeySchema": [
                    {
                        "AttributeName": "country",
                        "KeyType": "HASH"
                    }
                ],
                "ProvisionedThroughput": {
                    "ReadCapacityUnits": 10,
                    "WriteCapacityUnits": 10
                },
                "Projection": {
                    "ProjectionType": "ALL"
                }
            }
        ],
        BillingMode = "PROVISIONED",
        ProvisionedThroughput = {
            "ReadCapacityUnits": 10,
            "WriteCapacityUnits": 10
        },
        StreamSpecification = {
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES"
        }
    )

    table.wait_until_exists()

    user = {
        "id": "123456789",
        "name": "Mateo",
        "lastName": "Epalza Ramirez",
        "age": 24,
        "email": "test@gmail.com",
        "phone": 3124106058,
        "country": "Colombia"
    }

    table.put_item(
        TableName = "clients",
        Item= user
    )


if __name__ == "__main__":
    create_table()