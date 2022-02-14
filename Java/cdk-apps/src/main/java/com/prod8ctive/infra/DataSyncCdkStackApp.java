package com.prod8ctive.infra;

import static com.prod8ctive.infra.common.CdkConstants.CDK_PROPERTY_ENV_NAME;
import static com.prod8ctive.infra.common.CdkConstants.CDK_PROPERTY_MODULE_NAME;
import static com.prod8ctive.infra.common.CdkConstants.CDK_PROPERTY_ZONE_NAME;
import static com.prod8ctive.infra.common.CdkConstants.CDK_PROPERTY_QUALIFIER;
import static com.prod8ctive.infra.common.CdkConstants.CONTEXT_KEY_QUALIFIER;
import static com.prod8ctive.infra.common.CdkConstants.CONTEXT_KEY_AWS_ENV_NAME;
import static com.prod8ctive.infra.common.CdkConstants.CONTEXT_KEY_MODULE_NAME;
import static com.prod8ctive.infra.common.CdkConstants.CONTEXT_KEY_ZONE_NAME;
import static com.prod8ctive.infra.common.CdkConstants.DEFAULT_QUALIFIER;
import static com.prod8ctive.infra.common.CdkConstants.DEFAULT_ENV;
import static com.prod8ctive.infra.common.CdkConstants.DEFAULT_ZONE;

import java.util.HashMap;
import java.util.Map;

import com.prod8ctive.infra.common.AbstractCdkBuildingBlock;

import software.amazon.awscdk.App;
import software.amazon.awscdk.AppProps;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awssdk.utils.StringUtils;


/**
 * @author CHAWL001
 */
public final class DataSyncCdkStackApp {
	
	public static final String APP_NAME = "datasync";
	
    public static void main(final String[] args) {
    	Map<String, ? extends Object> datasyncContext = buildContext();
    	datasyncContext.entrySet()
    		.forEach(e -> System.err.println(String.format("[Context-set] key=%s value=%s", e.getKey(), e.getValue())));
    	
    	AppProps appProps = AppProps.builder()
    			.analyticsReporting(true)
    			.treeMetadata(true)
    			.stackTraces(true)
    			.context(datasyncContext)
    			.build();
        App app = new App(appProps);
        
        Environment datasyncEnv = AbstractCdkBuildingBlock.makeEnv(null, null);
        
        System.out.println("[ENV] CDK Account : "+ datasyncEnv.getAccount());
        System.out.println("[ENV] CDK Region  : "+ datasyncEnv.getRegion());
        
        final String cdkQualifier 	= AbstractCdkBuildingBlock.extractCdkQualifier(app);
        final String cdkEnvName 	= AbstractCdkBuildingBlock.extractCdkEnvName(app);
        final String cdkModuleName 	= AbstractCdkBuildingBlock.extractCdkModuleName(app);
        final String zoneName   	= AbstractCdkBuildingBlock.extractCdkZoneName(app);
        
        System.out.println("[Context-get] CDK EnvName 		: "+ cdkEnvName);
        System.out.println("[Context-get] CDK ModuleName  	: "+ cdkModuleName);
        
//        final StackProps nestedStackProps = buildStackProps(datasyncEnv, cdkQualifier, cdkEnvName, cdkModuleName, zoneName, APP_NAME, null);
        
        final StackProps glueModuleEnvSP = buildStackProps(datasyncEnv, cdkQualifier, cdkEnvName, cdkModuleName, zoneName, APP_NAME, GlueCdk.class);
        final String GLUE_STACK_ID = String.format("Linear-%s-%s-GlueCdkStack-%s-%s", cdkQualifier, cdkModuleName, zoneName, cdkEnvName);
        Stack cdkGlueStack = new GlueCdk(app, GLUE_STACK_ID, glueModuleEnvSP, true);
        
        final StackProps kdfModuleEnvSP = buildStackProps(datasyncEnv, cdkQualifier, cdkEnvName, cdkModuleName, zoneName, APP_NAME, FirehoseCdk.class);
        final String FIREHOSE_STACK_ID = String.format("Linear-%s-%s-FirehoseCdkStack-%s-%s", cdkQualifier, cdkModuleName, zoneName, cdkEnvName);
        Stack cdkFirehoseStack = new FirehoseCdk(app, FIREHOSE_STACK_ID, kdfModuleEnvSP);
        
        // Add dependencies
        cdkFirehoseStack.getNode().addDependency(cdkGlueStack);
        
        final StackProps lambdaModuleEnvSP = buildStackProps(datasyncEnv, cdkQualifier, cdkEnvName, cdkModuleName, zoneName, APP_NAME, LambdaCdk.class);
        final String LAMBDA_STACK_ID = String.format("Linear-%s-%s-LambdaCdkStack-%s-%s", cdkQualifier, cdkModuleName, zoneName, cdkEnvName);
		Stack cdkLambdaStack = new LambdaCdk(app, LAMBDA_STACK_ID, lambdaModuleEnvSP);
		
        // Add dependencies
        cdkLambdaStack.getNode().addDependency(cdkFirehoseStack);
        
        System.out.println("App: \n A/C: "+ app.getAccount() +"- Region: "+ app.getRegion() +"- Artifact: "+ app.getArtifactId() +"- Stage: "+ app.getStageName());
        System.out.println("Glue Stack:\n A/C: "+ cdkGlueStack.getAccount() +"- Region: "+ cdkGlueStack.getRegion() +"- Artifact: "+ cdkGlueStack.getArtifactId() +"- StackId: "+ cdkGlueStack.getStackId() +"- StackName: "+ cdkGlueStack.getStackName() +"- Env: "+ cdkGlueStack.getEnvironment());
        System.out.println("KDF Stack:\n A/C: "+ cdkFirehoseStack.getAccount() +"- Region: "+ cdkFirehoseStack.getRegion() +"- Artifact: "+ cdkFirehoseStack.getArtifactId() +"- StackId: "+ cdkFirehoseStack.getStackId() +"- StackName: "+ cdkFirehoseStack.getStackName() +"- Env: "+ cdkFirehoseStack.getEnvironment());
        System.out.println("Lambda Stack:\n A/C: "+ cdkLambdaStack.getAccount() +"- Region: "+ cdkLambdaStack.getRegion() +"- Artifact: "+ cdkLambdaStack.getArtifactId() +"- StackId: "+ cdkLambdaStack.getStackId() +"- StackName: "+ cdkLambdaStack.getStackName() +"- Env: "+ cdkLambdaStack.getEnvironment());
        
        app.synth();
    }

    private static Map<String, ? extends Object> buildContext() {
    	Map<String,String> dataSyncContextMap = new HashMap<>();
    	
    	String cdk_qualifier = System.getenv(CDK_PROPERTY_QUALIFIER);
    	if (StringUtils.isBlank(cdk_qualifier)) cdk_qualifier = DEFAULT_QUALIFIER;
    	
    	String cdk_env = System.getenv(CDK_PROPERTY_ENV_NAME);
    	if (StringUtils.isBlank(cdk_env)) cdk_env = DEFAULT_ENV;
    	
    	String cdk_module = System.getenv(CDK_PROPERTY_MODULE_NAME);
    	if (StringUtils.isBlank(cdk_module)) cdk_module = "con-common";
    	
    	String cdk_zone = System.getenv(CDK_PROPERTY_ZONE_NAME);
    	if (StringUtils.isBlank(cdk_zone)) cdk_zone = DEFAULT_ZONE;
    	
    	dataSyncContextMap.put(CONTEXT_KEY_QUALIFIER, cdk_qualifier);
    	dataSyncContextMap.put(CONTEXT_KEY_AWS_ENV_NAME, cdk_env);
    	dataSyncContextMap.put(CONTEXT_KEY_MODULE_NAME, cdk_module);
    	dataSyncContextMap.put(CONTEXT_KEY_ZONE_NAME, cdk_zone);
    	
    	return dataSyncContextMap;
    }
    
	private static StackProps buildStackProps(Environment datasyncEnv, 
			final String cdkQualifier,
			final String cdkEnvName,
			final String moduleName,
			final String zoneName,
			final String mainStackName,
			final Class<? extends AbstractCdkBuildingBlock> cdkStackClass) 
	{
		final String stackName = (null == cdkStackClass) ? mainStackName : cdkStackClass.getSimpleName();
		
		String description = String.format("Nested stack %s for context {%s:%s, %s:%s, %s:%s, %s:%s}", mainStackName, 
				CONTEXT_KEY_QUALIFIER, cdkQualifier,
				CONTEXT_KEY_AWS_ENV_NAME, cdkEnvName,
				CONTEXT_KEY_MODULE_NAME, moduleName,
				CONTEXT_KEY_ZONE_NAME, zoneName
		);
		
		final String effectiveStackName = String.format("laap-%s-%s-%s-%s-stack", stackName, cdkQualifier, AbstractCdkBuildingBlock.hyphenate(moduleName), zoneName);
		final Map<String,String> tagsMap = AbstractCdkBuildingBlock.getTagsAsMap(cdkQualifier, moduleName, cdkEnvName, zoneName, cdkStackClass);
        final StackProps baseStackProps =  StackProps.builder()
        		.env(datasyncEnv)
        		.stackName(effectiveStackName)
        		.terminationProtection(false)
        		.tags(tagsMap)
        		.description(description)
        		.build();
		return baseStackProps;
	}    

}
