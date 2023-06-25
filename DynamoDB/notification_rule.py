import boto3

# Create a CodeStarNotifications client
aws_session = boto3.session.Session(region_name="us-east-1", profile_name="Tungurahua")
client = aws_session.client('codestar-notifications')

# List notification rules

paginator = client.get_paginator('list_notification_rules')

for page in paginator.paginate():
    for rule in page['NotificationRules']:
        print(rule)
        rule_detail = client.describe_notification_rule(Arn=rule['Arn'])
        if rule_detail['Name'] == "spa-backoffice-main_release_14020_qa":
            print("Found the rule: ", rule['Arn'])
            client.delete_notification_rule(arn=rule['Arn'])
            print("Deleted the rule: ", rule['Arn'])
            break