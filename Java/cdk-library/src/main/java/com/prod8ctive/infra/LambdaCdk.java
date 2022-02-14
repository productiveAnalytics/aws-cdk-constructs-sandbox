package com.prod8ctive.infra;

import static com.prod8ctive.infra.common.CdkConstants.CDK_ID_CFN_OUTPUT_QUALIFIER;
import static com.prod8ctive.infra.common.CdkConstants.LAMBDA_TRIGGER_LIMIT_100;
import static com.prod8ctive.infra.common.CdkConstants.LAMBDA_TRIGGER_LIMIT_500;

import com.prod8ctive.infra.common.AbstractCdkBuildingBlock;
import com.prod8ctive.infra.common.FolderTraverser;
import com.prod8ctive.infra.config.LambdaConfig;
import com.prod8ctive.infra.util.YamlConfigProcessor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awscdk.*;
import software.amazon.awscdk.services.ec2.*;
import software.amazon.awscdk.services.iam.IRole;
import software.amazon.awscdk.services.iam.Role;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.lambda.*;
import software.amazon.awscdk.services.lambda.eventsources.AuthenticationMethod;
import software.amazon.awscdk.services.lambda.eventsources.SelfManagedKafkaEventSource;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.amazon.awscdk.services.secretsmanager.ISecret;
import software.amazon.awscdk.services.secretsmanager.Secret;
import software.amazon.awscdk.services.ssm.ParameterTier;
import software.amazon.awscdk.services.ssm.StringParameter;
import software.amazon.awssdk.utils.Pair;
import software.constructs.Construct;

import java.util.*;
import java.util.stream.Collectors;


public class LambdaCdk extends AbstractCdkBuildingBlock {
    public LambdaCdk(final Construct parent, final String id) {
        this(parent, id, null);
    }

    public LambdaCdk(final Construct parent, final String id, /* final ModuleEnvEnabledStackProps moduleEnvStackProps */ final StackProps moduleEnvStackProps) {
        super(parent, id, moduleEnvStackProps);
        
        ObjectMapper mapper = new ObjectMapper();
        String moduleName = getModuleName();
        String zoneName = getZoneName();
        final String cdkEnv = getCdkEnvName();
        
        final String roleId = getRoleId();

        FolderTraverser<Pair<String,String>> topicsFolderTraverser = new FolderTraverser<>();
        LambdaConfig lambdaConfig = YamlConfigProcessor.loadLambdaConfig(cdkEnv);

        ILayerVersion layer1 = LayerVersion.fromLayerVersionArn(this, lambdaConfig.getConfluentLayerName(),
                lambdaConfig.getConfluentLayerArn());
        ILayerVersion layer2 = LayerVersion.fromLayerVersionArn(this, lambdaConfig.getPythonLayerName(),
                lambdaConfig.getPythonLayerArn());

        IRole role = Role.fromRoleArn(this, roleId, lambdaConfig.getLambdaRoleArn());

        ISecurityGroup securityGroup = SecurityGroup.fromSecurityGroupId(this, lambdaConfig.getSecurityGroupId(),
                lambdaConfig.getSecurityGroupId());

        IVpc vpc = Vpc.fromVpcAttributes(this, lambdaConfig.getVpcId(), VpcAttributes.builder()
                .vpcId(lambdaConfig.getVpcId())
                .privateSubnetIds(lambdaConfig.getPrivateSubnetIds())
                .availabilityZones(lambdaConfig.getAvailabilityZones()).build());

        SubnetSelection vpcSubnets = SubnetSelection.builder().subnets(
                lambdaConfig.getEventSourceTriggerVpcSubnets().stream().map(subnet ->
                        Subnet.fromSubnetId(this, subnet, subnet)).collect(Collectors.toList())).build();

        ISecret secret = Secret.fromSecretCompleteArn(this, lambdaConfig.getKafkaConnectionSecretName(), lambdaConfig.getKafkaConnectionSecretArn());

        final String FUNCTION_NAME = String.format("laap_lambda_kafka2firehose_%s_%s_%s_%s", getCdkQualifier(), moduleName, zoneName, cdkEnv);

        String schemaFilePath;
        String kafkaTopicname;
//        String exportedTriggerName;
        String firehoseName;
        String topic2FirehoseJson = null;
        List<SelfManagedKafkaEventSource> selfManagedKafkaEventSourceList = new ArrayList<>();
        Map<String, String> topic2FirehoseMap = new HashMap<>();

        for (Pair<String, String> stringStringPair : topicsFolderTraverser) {
            schemaFilePath = stringStringPair.right();
            kafkaTopicname = extractKafkaTopic(extractGlueSchemaFile(schemaFilePath));
            firehoseName = getFirehoseName(kafkaTopicname);
            topic2FirehoseMap.put(kafkaTopicname, firehoseName);
            
//            exportedTriggerName = String.format("datasync-lambda-trigger-for-%s", hyphenate(kafkaTopicname));
//            CfnOutput.Builder.create(this,
//            		String.format("cfn-output-%s-trigger-%s", FUNCTION_NAME, kafkaTopicname))
//                .exportName(exportedTriggerName)
//                .description("DataSync Lambda trigger for topic: "+ kafkaTopicname)
//                .value(kafkaTopicname)
//                .build();
            selfManagedKafkaEventSourceList.add(getSelfManagedKafkaEventSource(lambdaConfig.getKafkaBootstrapServerList(), kafkaTopicname, secret, vpc, vpcSubnets, securityGroup));
        }
        try {
            topic2FirehoseJson = mapper.writeValueAsString(topic2FirehoseMap);
            System.out.println("TOPIC_2_FIREHOSE_JSON: " + topic2FirehoseJson);
        } catch (JsonProcessingException e) {
            System.err.println("KafkaTopic2FirehoseMap conversion error: " + e.getMessage());
            System.exit(1);
        }
        Map<String, String> environment = new HashMap<>();

        final StringParameter ssmParam = createSSMParameter(topic2FirehoseJson);
        environment.put("LOGGING_LEVEL", "INFO");	// Py Logging levels: DEBUG < INFO < WARNING < ERROR < CRITICAL
        environment.put("SSM_PARAM_NAME", ssmParam.getParameterName());
        environment.put("SCHEMA_REGISTRY_URL", lambdaConfig.getSchemaRegistryUrl());
        environment.put("SCHEMA_REGISTRY_SECRET_NAME", lambdaConfig.getSchemaRegistryConnectionSecretName());

        Function linearLambda = new Function(this, FUNCTION_NAME, FunctionProps.builder()
        		.functionName(FUNCTION_NAME)
                .runtime(Runtime.PYTHON_3_8)
                .code(Code.fromAsset(lambdaConfig.getLambdaFilePath()))
                .role(role)
                .handler("lambda_function.lambda_handler")
                .layers(Arrays.asList(layer1, layer2))
                .vpc(vpc)
                .securityGroups(Collections.singletonList(securityGroup))
                .memorySize(1024)
                .environment(environment)
                //TODO check this
                .timeout(Duration.minutes(5))
                .logRetention(RetentionDays.TWO_WEEKS)
                .build());

        selfManagedKafkaEventSourceList.forEach(linearLambda::addEventSource);
        
        // Allow Lambda to read SSM 
        ssmParam.grantRead(linearLambda);
        
        
        String _MODULE_NAME = hyphenate(moduleName);
        String EXPORTED_LAMBDA_CDK_ID = String.format( (CDK_ID_CFN_OUTPUT_QUALIFIER+"-%s-%s-arn"), getCdkQualifier(), _MODULE_NAME, getZoneName(), linearLambda.getRuntime(), FUNCTION_NAME).replace("_", "-").replace(".", "-");
        System.out.println("lambdaExportId: " + EXPORTED_LAMBDA_CDK_ID);

        CfnOutput.Builder.create(this, EXPORTED_LAMBDA_CDK_ID)
            .exportName(EXPORTED_LAMBDA_CDK_ID)
            .description("DataSync Lambda ARN")
            .value(linearLambda.getFunctionArn())
            .build();
    }

    private SelfManagedKafkaEventSource getSelfManagedKafkaEventSource(List<String> bootstrapServerList,
                                                                       String topicName,
                                                                       ISecret secret,
                                                                       IVpc vpc,
                                                                       SubnetSelection vpcSubnets,
                                                                       ISecurityGroup securityGroup) {
        return SelfManagedKafkaEventSource.Builder.create()
                .bootstrapServers(bootstrapServerList)
                .topic(topicName)
                .secret(secret)
                .authenticationMethod(AuthenticationMethod.SASL_SCRAM_512_AUTH)
                .vpc(vpc)
                .vpcSubnets(vpcSubnets)
                .securityGroup(securityGroup)
                .batchSize(LAMBDA_TRIGGER_LIMIT_100) // default
                .startingPosition(StartingPosition.TRIM_HORIZON)
                .enabled(false)
                .build();
    }
    
    private StringParameter createSSMParameter(final String jsonValue) {
    	String MODULE_NAME = underscore(getModuleName()).toUpperCase();
    	String ZONE_NAME = getZoneName();
    	String _MODULE_NAME = hyphenate(getModuleName());
    	String ENV_NAME = getCdkEnvName().toUpperCase();
    	
    	final String MODULE_ENV_SSM_PARAM_NAME = String.format("TOPIC_2_FIREHOSE_JSON_%s_%s_%s_%s", getCdkQualifier(), MODULE_NAME, ZONE_NAME, ENV_NAME);
        final String SSM_PARAM_CDK_ID = String.format("ssm-param-for-lambda-%s-%s-%s-%s", getCdkQualifier(), _MODULE_NAME, ZONE_NAME, getCdkEnvName());
        
    	StringParameter ssmParamForLambda = StringParameter.Builder.create(this, SSM_PARAM_CDK_ID)
    			.parameterName(MODULE_ENV_SSM_PARAM_NAME)
    			.stringValue(jsonValue)
    			.tier(ParameterTier.ADVANCED)
    			.description("Topic to Firehose map as JSON")
    			.build();
    	
    	final String EXPORTED_SSM_PARAM_CDK_ID= String.format(CDK_ID_CFN_OUTPUT_QUALIFIER+"-lambda-arn", getCdkQualifier(), _MODULE_NAME, ZONE_NAME);
    	CfnOutput.Builder.create(this, EXPORTED_SSM_PARAM_CDK_ID)
	        .exportName(EXPORTED_SSM_PARAM_CDK_ID)
	        .description("ARN of SSM Parameter for Lambda")
	        .value(ssmParamForLambda.getParameterArn())
	        .build();
    		
    	return ssmParamForLambda;
    }
}
