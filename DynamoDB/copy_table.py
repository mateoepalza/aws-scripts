import boto3
import json
ENVIRONMENT = "qa"
def get_aws_connection(region, pr_name, environment):
    aws_session = boto3.session.Session(region_name=region, profile_name=pr_name)
    aws_dynamodb = aws_session.resource('dynamodb')
    dynamo_table = aws_dynamodb.Table("usrv-transaction-retails-"+environment+"-equivalences")
    return aws_session, aws_dynamodb, dynamo_table
def create_items():
    _, _, table = get_aws_connection("us-east-1", "Dev", "uat")
    au_paths = table.scan()
    count = 0
    for i in au_paths['Items']:
        count = count + 1
        putItem(i)
    print("total")
    print(count)
def putItem(item):
    _, _, table = get_aws_connection("us-east-1","Orizaba", "qa")
    table.put_item(Item=item)
if __name__ == '__main__':
    create_items()