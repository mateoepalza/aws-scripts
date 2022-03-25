import boto3

dynamodb = boto3.resource("dynamodb")

def put_item(tableName, item):
    dynamodb.Table(tableName).put_item(Item=item)

if __name__ == "__main__":
    tableName = "clients"
    user = {
        "id": "9876532",
        "name": "Juan",
        "lastName": "Pedraza Ramirez",
        "age": 30,
        "email": "test@gmail.com",
        "phone": 654321654,
        "country": "Peru"
    }
    put_item(tableName, user)