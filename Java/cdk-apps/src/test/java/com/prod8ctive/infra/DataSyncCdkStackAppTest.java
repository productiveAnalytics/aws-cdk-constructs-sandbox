package com.prod8ctive.infra;

import software.amazon.awscdk.App;
import software.amazon.awscdk.AppProps;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.assertions.Template;
import software.amazon.awssdk.regions.Region;
import software.amazon.awscdk.assertions.Match;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.prod8ctive.infra.common.AbstractCdkBuildingBlock;
import com.prod8ctive.infra.common.ModuleEnvEnabledStackProps;

import static org.junit.jupiter.api.Assertions.*;
import static com.prod8ctive.infra.common.CdkConstants.CONTEXT_KEY_AWS_ENV_NAME;
import static com.prod8ctive.infra.common.CdkConstants.CONTEXT_KEY_MODULE_NAME;

/**
 * 
 * @author chawl001
 */
public class DataSyncCdkStackAppTest {
	static String TEST_ACCOUNT_SBX 	= "887847050650";		// TODO: take from test property file
	static String TEST_REGION_EC1  	= Region.EU_CENTRAL_1.id();
	static String TEST_AWS_ENV_NAME = "sbx";
	static String TEST_MODULE_NAME  = "con-test";
	
    @Test
//    @Disabled ("Getting weird error even thoug set CDK_DEFAULT_ACCOUNT and CDK_DEFAULT_REGION")
    public void testStack() throws IOException {

    	Map<String,String> testContextMap = new HashMap<>();
    	testContextMap.put(CONTEXT_KEY_AWS_ENV_NAME, TEST_MODULE_NAME);
    	testContextMap.put(CONTEXT_KEY_MODULE_NAME, TEST_AWS_ENV_NAME);
    	
        final App testApp = new App(AppProps.builder().context(testContextMap).build());
        
        Environment testDatasyncEnv = AbstractCdkBuildingBlock.makeEnv(TEST_ACCOUNT_SBX, TEST_REGION_EC1);
        
        System.out.println("[TEST] CDK Account : "+ testDatasyncEnv.getAccount());
        System.out.println("[TEST] CDK Region  : "+ testDatasyncEnv.getRegion());
        
        final StackProps testDatasyncStackProps =  StackProps.builder().env(testDatasyncEnv).build();
        
        final ModuleEnvEnabledStackProps moduleEnvStackProps = ModuleEnvEnabledStackProps.builder()
        		.stackProps(testDatasyncStackProps)
        		.cdkEnvName(TEST_AWS_ENV_NAME)
        		.moduleName(TEST_MODULE_NAME)
        		.build();
        
        Environment testEnv = moduleEnvStackProps.getEnv();
        assertNotNull(testEnv);
        assertEquals(testEnv.getAccount(), TEST_ACCOUNT_SBX, "Expected test AWS account Id to match");
        assertEquals(testEnv.getRegion(), "eu-central-1", "Expected test AWS region to be eu-central-1");
        
        Stack cdkGlueStack = new GlueCdk(testApp, "TestLinearGlueCdkStack", moduleEnvStackProps, true);
        assertEquals(cdkGlueStack.getAccount(), TEST_ACCOUNT_SBX);
        assertEquals(cdkGlueStack.getRegion(), TEST_REGION_EC1);

        Template glueTemplate = Template.fromStack(cdkGlueStack);
        System.err.println("Glue stack "+ glueTemplate.toJSON());
//        glueTemplate.resourceCountIs("AWS::Glue::Table", 1);
        
        String conformanceGlueDB  = moduleEnvStackProps.getDatabaseIdConformance();
        Map<String,Object> glueDbPropsMap = 
        		Collections.singletonMap("Properties",
	        		Collections.singletonMap("DatabaseInput",
	        								Collections.singletonMap("Name", conformanceGlueDB))
	        	);
        glueTemplate.hasResource("AWS::Glue::Database", glueDbPropsMap);
        
        Map<String,Object> glueTablePropsMap = 
        		Collections.singletonMap("Properties",
        				Collections.singletonMap("DatabaseName", conformanceGlueDB));
        Map<String,Map<String,Object>> glueTablesMap = glueTemplate.findResources("AWS::Glue::Table", glueTablePropsMap);
        final int totalGlueTablesCount = glueTablesMap.keySet().size();
        
        assertTrue(totalGlueTablesCount > 0, "Expected at-least one Glue table");
        
        Stack cdkFirehoseStack = new FirehoseCdk(testApp, "TestLinearFirehoseCdkStack", moduleEnvStackProps);
        cdkFirehoseStack.getNode().addDependency(cdkGlueStack);
        
        Template firehoseTemplate = Template.fromStack(cdkFirehoseStack);
        System.err.println("KDF stack "+ firehoseTemplate.toJSON());
//        firehoseTemplate.resourceCountIs("AWS::KinesisFirehose::DeliveryStream", 1);
        
        glueTablesMap.entrySet().stream()
        	.forEach(glueTbl -> {
        		String glueTableName = glueTbl.getKey();
        		
        		// Destinations.ExtendedS3DestinationDescription.DataFormatConversionConfiguration.SchemaConfiguration.TableName
        		Map<String,String> m1 = Collections.singletonMap("TableName", glueTableName);
        		Map<String,Object> m2 = Collections.singletonMap("SchemaConfiguration", m1);
        		Map<String,Object> m3 = Collections.singletonMap("DataFormatConversionConfiguration", m2);
        		Map<String,Object> m4 = Collections.singletonMap("ExtendedS3DestinationDescription", m3);
        		Map<String,Object> firehosePropMap = Collections.singletonMap("Destinations", m4);
        		firehoseTemplate.hasResource("AWS::KinesisFirehose::DeliveryStream", firehosePropMap);
        	});
        
        Map<String,Map<String,Object>> resourceMap = firehoseTemplate.findResources("AWS::KinesisFirehose::DeliveryStream");
        System.out.println(resourceMap);
        
        String firehoseName = resourceMap.keySet().stream().findFirst().get();
        Map<String,Object> firehoseDetails = resourceMap.get(firehoseName);
        assertNotNull(firehoseDetails, "Info of Kinesis Firehose must not be null");
        Object firehoseProps = firehoseDetails.get("Properties");
        assertTrue(firehoseProps instanceof Map, "Expecting Properties as Map");
        Map<String,Object> fireshosePropMap = (Map<String,Object>) firehoseProps;
        assertEquals("DirectPut", fireshosePropMap.get("DeliveryStreamType"), "Kinesis Firehose DeliveryStreamType must not be: DirectPut");
    }
}
