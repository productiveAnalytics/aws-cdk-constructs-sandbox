# Application with Glue and Firehose constructs from linear-cdk

## Download Glue Schema (CSV) files from S3
```
aws s3 cp --recursive <s3-path-for-folder-conataining-output-of-AVRO-2-Glue-lambda> ./src/main/resources/cdk_test/
```

## Build project (skipping tests for speedy build)
```
mvn clean package -DskipTests
(or for local): mvn --offline clean install -DskipTests
```

## Ensure that environment variables are set for CDK e.g. CDK_MODULE_NAME that refers to Linear module 
```
export CDK_NEW_BOOTSTRAP=1
export CDK_DEBUG=true
export CDK_DEFAULT_ACCOUNT=<aws_account_id-e.g.887847050650>
export CDK_DEFAULT_REGION=<aws_region_id-e.g.us-east-1>
```

## Ensure that context variables are set for CDK 
```
export CDK_CONTEXT_QUALIFIER=datasync04
export CDK_CONTEXT_ENV_NAME=sbx
export CDK_CONTEXT_MODULE_NAME=con-common
```

Run following command to ensure environment variables are set for CDK
```
cdk doctor
```

## Bootstrap CDK for AWS account and Region (Note: qualifier should match with qualifier specified in cdk.json)
```
cdk bootstrap aws://${CDK_DEFAULT_ACCOUNT}/${CDK_DEFAULT_REGION} \
--profile default \
--trust 887847050650 --trust-for-lookup 887847050650 \
--cloudformation-execution-policies arn:aws:iam::887847050650:policy/dtci-admin \
--qualifier datasync03 \
--toolkit-stack-name cdk-toolkit-datasync-datasync03-stack \
--tags author=LDC --tags usage=datasync --tags module=con-cust --tags qualifier=datasync03 \
--force
```

Check the CDK bootstrapping template:
```
cdk bootstrap \
--toolkit-stack-name cdk-toolkit-datasync-datasync03-stack \
--show-template  > ./bootstrap_template_datasync03.yaml
```

## Debug CDK stack
Check stack(s) in this CDK application:
```
cdk \
--toolkit-stack-name cdk-toolkit-datasync-datasync03-stack \
ls
```
Ouput would be, like:
Linear-datasync03-con-common-GlueCdkStack-sbx
Linear-datasync03-con-common-LambdaRoleCdkStack-sbx
Linear-datasync03-con-common-FirehoseCdkStack-sbx
Linear-datasync03-con-common-LambdaCdkStack-sbx

Check the change-set:
```
cdk \
--toolkit-stack-name cdk-toolkit-datasync-datasync03-stack \
--staging \
diff
```

## Glue Database format pattern (e.g. module-name=con-common, env-name=sbx):
- Landing: laap-glue-db-landing-<env-name>
- Conformance: laap-glue-db-conformance-<module-name>-<env-name>

## Deploy CDK stack along with new Glue Database
```
cdk \
--toolkit-stack-name cdk-toolkit-datasync-datasync03-stack \
--parameters rolename=laap-role-data-gen-firehose-dev \
--parameters topicss3bucket=laap-ue1-gen-repository-sbx --parameters topicsfolder=not_needed_for_local_schemas \
--staging \
--require-approval never \
--progress events \
deploy --all
```


----

## To manually delete stacks:
1. Disable termination protection for stack
```
aws cloudformation update-termination-protection \
--no-enable-termination-protection \
--stack-name datasync-con-cust-base-stack-sbx
```
2. Destroy full-stack:
```
cdk \
--toolkit-stack-name cdk-toolkit-datasync-datasync03-stack \
destroy --all
```

----

#TODO
[X] Move actual zip of lambda from linear-cdk-apps/lambda_functions/laap-lambda-kafka-to-firehose/lamda_functions.py.zip to libary-side: linear-cdk/src/main/resources/
