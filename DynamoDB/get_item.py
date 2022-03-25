import boto3

dynamodb = boto3.resource("dynamodb")

def get_item(tableName, id):
    result = dynamodb.Table(tableName).get_item(
        Key = {
            "id": id
        }
    )
    print(result["Item"])

if __name__ == "__main__":
    tableName = "clients"
    id = "123456789"
    get_item(tableName, id)