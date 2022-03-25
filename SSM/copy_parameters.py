import boto3
import json


FROM = "Dev"
TO = "Reventador"
FROM_PATH = "/ci/usrv-bank-conciliation/"
OVERWRITE = False # This will overwite all the parameters if already exists
REGION = "us-east-1"
FUNCTION_NAME = "usrv-kushki-ci-dev-microServiceDeploy"
MICRO_NAME = "usrv-bank-conciliation"
BRANCH = "release/13035"

def get_session(profile, region, service):
    session = boto3.Session(profile_name=profile, region_name=region)
    return session.client(service)

def copy_parameters():
    client = get_session(FROM, REGION, "ssm") # From

    result = client.get_parameters_by_path(
        Path= FROM_PATH,
        Recursive= True
    )
    return result

def set_parameters(params):
    client = get_session(TO, REGION, "ssm") # To

    for res in params["Parameters"]:
        try:
            client.put_parameter(
                Name = res['Name'].replace("/ci/", "/qa/"),
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
    set_parameters(params)
    call_deploy()


if __name__ == "__main__":
    main()