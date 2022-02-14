package com.prod8ctive.infra;

import com.prod8ctive.infra.common.AbstractCdkBuildingBlock;
import com.prod8ctive.infra.common.ModuleEnvEnabledStackProps;
import com.prod8ctive.infra.config.LambdaConfig;
import com.prod8ctive.infra.util.YamlConfigProcessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.iam.*;
import software.constructs.Construct;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Arrays;
import java.util.stream.Collectors;

import static com.prod8ctive.infra.common.CdkConstants.*;

public class RoleCdk extends AbstractCdkBuildingBlock {
    public RoleCdk(final Construct parent, final String id) {
        this(parent, id, null);
    }
    public RoleCdk(Construct parent, String id, /* final ModuleEnvEnabledStackProps moduleEnvStackProps */ final StackProps moduleEnvStackProps) {
        super(parent, id, moduleEnvStackProps);
        
        String _MODULE_NAME = hyphenate(getModuleName());
        final String cdkEnv = getCdkEnvName();
        LambdaConfig lambdaConfig = YamlConfigProcessor.loadLambdaConfig(cdkEnv);
        String roleId = String.format(LAMBDA_ROLE_PREFIX, getCdkEnvName());

        String policyStatementId = LAMBDA_POLICY_STATEMENT_PREFIX + cdkEnv;
        String lambdaPolicyDocumentFile = lambdaConfig.getLambdaPolicyDocumentFilePath().replace("<env>", cdkEnv);

        ManagedPolicy managedPolicy = ManagedPolicy.Builder.create(this, policyStatementId)
                .description("Policy statement for Data Sink Lambda execution")
                .managedPolicyName(policyStatementId)
                .document(readPolicyDocument(lambdaPolicyDocumentFile))
                .build();

        System.out.println("Created Policy. ARN: " + managedPolicy.getManagedPolicyArn());

        IRole role = Role.Builder.create(this, roleId)
                .roleName(roleId)
                .assumedBy(new ServicePrincipal("lambda.amazonaws.com"))
                .description("Allows Lambda function role to call AWS services in " + cdkEnv + " environment")
                .managedPolicies(Arrays.asList(
                    ManagedPolicy.fromAwsManagedPolicyName("AmazonEC2FullAccess"),
                    ManagedPolicy.fromAwsManagedPolicyName("AmazonKinesisFirehoseFullAccess"),
                    ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaBasicExecutionRole"),
                    ManagedPolicy.fromAwsManagedPolicyName("service-role/AWSLambdaVPCAccessExecutionRole"),
                    ManagedPolicy.fromAwsManagedPolicyName("AmazonCodeGuruProfilerAgentAccess"),
                    ManagedPolicy.fromManagedPolicyName(this, policyStatementId, managedPolicy.getManagedPolicyName())
                    )
                )
                .build();

        System.out.println("Created Role ARN: " + role.getRoleArn());

        String EXPORTED_ROLE_CDK_ID = String.format(CDK_ID_CFN_OUTPUT_QUALIFIER, getCdkQualifier(), _MODULE_NAME, getZoneName()) + "-"  + roleId;
        CfnOutput.Builder.create(this, EXPORTED_ROLE_CDK_ID)
        		.exportName(roleId)	
                .description("Role for " + roleId)
                .value(role.getRoleArn())
                .build();
    }

    private PolicyDocument readPolicyDocument(String lambdaPolicyDocumentFile) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String policyDocumentString = new BufferedReader(new FileReader(lambdaPolicyDocumentFile)).lines().collect(Collectors.joining(System.lineSeparator()));
            System.out.println("policyDocumentString: " + policyDocumentString);
            return PolicyDocument.fromJson(objectMapper.readTree(policyDocumentString));
        } catch (Exception e) {
            System.err.println("Unable to read policy document: "+ e.getMessage());
            System.exit(1);
        }
        return null;
    }
}
