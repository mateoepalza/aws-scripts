import boto3
import json


FROM = "Dev"
TO = "Orizaba"
FROM_PATH = "/qa/usrv-billing-charges/"
OVERWRITE = False # This will overwite all the parameters if already exists
REGION = "us-east-1"
FUNCTION_NAME = "usrv-kushki-ci-dev-microServiceDeploy"
MICRO_NAME = "usrv-billing-charges"
BRANCH = "rmaster"

def get_session(profile, region, service):
    session = boto3.Session(profile_name=profile, region_name=region)
    return session.client(service)

def copy_parameters():
    parameters = []
    client = get_session(FROM, REGION, "ssm") # From

    result = client.get_parameters_by_path(
        Path= FROM_PATH,
        Recursive= True
    )

    parameters = result["Parameters"]

    try:
        while(result["NextToken"] is not None):
            result = client.get_parameters_by_path(
                NextToken= result["NextToken"],
                Path= FROM_PATH,
                Recursive= True
            )
            print(result)
            parameters = parameters + result["Parameters"]
    except:
        print("There was an exception")

    return parameters

def set_parameters(params):
    client = get_session(TO, REGION, "ssm") # To
    print("HEREEE")
    print(params)
    
    for res in params:
        try:
            print(res['Name'])
            client.put_parameter(
                Name = res['Name'].replace("/qa/", "/qa/"),
                Value = res['Value'],
                Type = "String",
                Overwrite = OVERWRITE
            )
        except:
            print("This items was already existed", res["Name"], " : ", res["Value"])

def call_deploy():
    client = get_session(TO, REGION, "lambda")

    micro_object = '{"name": "%(micro)s", "branch": "%(branch)s", "stage": "qa"}' % {"micro" : MICRO_NAME, "branch": BRANCH}
    micro_string = json.dumps(micro_object)

    try:
        client.invoke(
            FunctionName = "usrv-kushki-ci-dev-microServiceDeploy",
            Payload = '{"body": %(micro)s }' % {"micro": micro_string}
        )
    except:
        print("There was an error executing the lambda")


def main():
    params = copy_parameters()
    print(params)
    set_parameters(params)
    #call_deploy()


if __name__ == "__main__":
    main()