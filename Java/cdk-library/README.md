# Welcome to CDK Java project for AWS Glue and Kinesis Data Firehose!

Multi-stack CDK  for Glue & Firehose

Install AWS CDK package:
`npm install -g aws-cdk@2.2.0`
`cdk --version`

To create Java project:
`cdk init --language java`

The `cdk.json` file tells the CDK Toolkit how to execute your app.

Check the AWS profile:
`aws sts get-caller-identity --profile default`

If needed, bootstrap cdk with advance parameter:
`cdk bootstrap --profile default --trust 887847050650 --trust-for-lookup 887847050650 --cloudformation-execution-policies arn:aws:iam::887847050650:policy/dtci-admin --qualifier ldccdk0001 --force`

It is a [Maven](https://maven.apache.org/) based project, so you can open this project with any Maven compatible Java IDE to build and run tests.

## Useful commands

 * `mvn package`     compile and run tests
 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy --all  --parameters rolename=laap-role-data-gen-firehose-dev --parameters gluedatabase=my_cdk_glue_db --parameters kafkatopic=my_cdk_glue_table --parameters s3bucket=laap-ue1-gen-landing-sbx --parameters basefolder=my_cdk_success_records --parameters errorfolder=my_cdk_error_records`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

Enjoy!
