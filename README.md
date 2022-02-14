# AWS CDK setup,  (Python, TypeScript and Java)

1. Install/Upgrade AWS CDK

1.a) Install AWS CDK
```bash
npm install -g aws-cdk 
# or use specific version like:  npm install -g aws-cdk@2.11.0
```
1.b) Create starter project
```bash
mkdir -p my-cdk/python/ && cd my-cdk/python/
cdk init --language python
```

2. Set-up generic CDK environments like AWS account and Region
```bash
export CDK_NEW_BOOTSTRAP=1
export CDK_DEBUG=true
export CDK_DEFAULT_ACCOUNT="<aws-account-id>"
export CDK_DEFAULT_REGION="us-east-1"
```

_Tip_ : Get account-id using `aws sts get-caller-identity`
```bash
MY_AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
if [[ -z "$MY_AWS_ACCOUNT_ID" ]]
then
  echo "Defaulting Account ID...Otherwise, first run: aws-saml-auth"
  MY_AWS_ACCOUNT_ID="<my-static-aws-account-id-here>"
else
  echo "aws sts get-caller-identity"
  aws sts get-caller-identity
fi
```

3. Set-up application-specific context parameters

_Note_: MUST match qualifier that is present in cdk.json present in the application-stack or update cdk.json with the qualifier you prefer. 

_Tip_:Refer to the contexts set by cdk.json, execute ```cdk context --json```
```bash
export CDK_CONTEXT_QUALIFIER=datasync
export CDK_CONTEXT_ENV_NAME=sbx
export CDK_CONTEXT_MODULE_NAME=con-test
export CDK_CONTEXT_ZONE_NAME=landing
```

4. Confirm AWS session
CDK command need AWS connection and access to S3, CFN execution role, etc. Ensure that the AWS session has been established and the executor has valid role. Most CDK commands stall and give a weird error, if the underlying AWS session has expired. For local, ensure to relogin using ```aws-saml-auth```

Confirm by: 
```bash
aws sts get-caller-identity
```

5. ONE TIME CDK bootstrap activity per account per region
_Note_: Note: Use _toolkit-stack-name_ that is specific for each stack
```bash
cdk bootstrap aws://${CDK_DEFAULT_ACCOUNT}/${CDK_DEFAULT_REGION} \
--trust 887847050650 --trust-for-lookup 887847050650 \
--cloudformation-execution-policies arn:aws:iam::887847050650:policy/dtci-admin \
--qualifier ${CDK_CONTEXT_QUALIFIER} \
--toolkit-stack-name laap-cdk-toolkit-datasync-${CDK_CONTEXT_QUALIFIER}-stack \
--tags author=LDC --tags usage=datasync --tags module=${CDK_CONTEXT_MODULE_NAME} --tags qualifier=${CDK_CONTEXT_QUALIFIER} \
--force
```

6. Confirm CDK environment settings
```bash
cdk doctor
```

7. Build Library / Building Blocks as a jar
```
mvn clean install -DskipTests
```

8. Build Application stack e.g. datasync stack, using the library jar
```
mvn --offline clean install -DskipTests
```

9. Checkout CDK stacks
```bash
cdk \
--toolkit-stack-name laap-cdk-toolkit-datasync-${CDK_CONTEXT_QUALIFIER}-stack \
--context qualifier=${CDK_CONTEXT_QUALIFIER} \
--context aws_env_name=${CDK_CONTEXT_ENV_NAME} \
--context module_name=${CDK_CONTEXT_MODULE_NAME} \
--context zone_name=${CDK_CONTEXT_ZONE_NAME} \
ls
```

The output looks like,
- My-datasync-con-test-landing-GlueCdkStack-sbx
- My-datasync07-con-test-landing-FirehoseCdkStack-sbx
- My-datasync07-con-test-landing-LambdaCdkStack-sbx

10. Check the desired changes / updates
```bash
cdk \
--toolkit-stack-name laap-cdk-toolkit-datasync-${CDK_CONTEXT_QUALIFIER}-stack \
--context qualifier=${CDK_CONTEXT_QUALIFIER} \
--context aws_env_name=${CDK_CONTEXT_ENV_NAME} \
--context module_name=${CDK_CONTEXT_MODULE_NAME} \
--context zone_name=${CDK_CONTEXT_ZONE_NAME} \
diff
```

11. Perform CDK deploy
```bash
cdk \
--toolkit-stack-name laap-cdk-toolkit-datasync-${CDK_CONTEXT_QUALIFIER}-stack \
--context qualifier=${CDK_CONTEXT_QUALIFIER} \
--context aws_env_name=${CDK_CONTEXT_ENV_NAME} \
--context module_name=${CDK_CONTEXT_MODULE_NAME} \
--context zone_name=${CDK_CONTEXT_ZONE_NAME} \
--parameters rolename=my-iam-role-data-gen-firehose-sbx \
--parameters topicss3bucket=my-s3-bucket-repository-sbx --parameters topicsfolder=not_needed_for_local_schemas \
--staging \
--require-approval never \
--progress events \
deploy --all
```
