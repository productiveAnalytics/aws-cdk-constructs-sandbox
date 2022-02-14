package com.disney.linear.infro;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.disney.linear.infra.common.AbstractCdkBuildingBlock;

import software.amazon.awscdk.Environment;
import software.amazon.awssdk.regions.Region;

/**
 * Test case for utility methods from Abstract Super Class
 * 
 * @author chawl001
 */
class AbstractCdkBuildingBlockTest {
	
	static String TEST_REGION 	= Region.US_WEST_2.id();
	static String TEST_ACCOUNT 	= "887847050650";
	
	static String ENV_REGION	= Region.EU_CENTRAL_1.id();
	static String ENV_ACCOUNT	= "123456789000";
	
	static String S3_BUCKET = "laap-ue1-gen-repository-sbx";
	static String TOPICS_FOLDER_WITH_DELIMETER = "avro_2_glue/output/";
	static String TOPIC_NAME = 
//			"lndcdc_ncs_tcs__flight_dates" 	/* -- GoldenGate topics */ 
			"lndcdcncstcs__flightdates" 	/* -- NiFi topics */ ;
	
	static final String TEST_MDOULE_NAME = "concust";

	@Test
	@Disabled ("Need to Mock CDK environment")
	void testMakeEnv() {
		Environment testEnvironment = AbstractCdkBuildingBlock.makeEnv(TEST_REGION, TEST_ACCOUNT);
		assertNotNull(testEnvironment, "Expected non-null CDK env");
		assertEquals(TEST_REGION, testEnvironment.getRegion());
		assertEquals(TEST_ACCOUNT, testEnvironment.getAccount());
		
		System.setProperty("CDK_DEFAULT_ACCOUNT", "dummy-accnt");
		System.setProperty("CDK_DEFAULT_REGION", null);
		try {
			Environment nullEnv = AbstractCdkBuildingBlock.makeEnv(null, null);
			fail("Should fail for setting null value for CDK_DEFAULT_REGION");
		} catch (AssertionError ae) {
			System.out.println("Don't worry...Expected to fail for null CDK_DEFAULT_REGION with message: "+ ae.getMessage());
		}
		
		System.setProperty("CDK_DEFAULT_ACCOUNT", ENV_ACCOUNT);
		System.setProperty("CDK_DEFAULT_REGION", "my-planet-1");
		try {
			Environment invalidEnvironment = AbstractCdkBuildingBlock.makeEnv(null, null);
			fail("Should fail for setting \"my-planet-1\" value for CDK_DEFAULT_REGION");
		} catch (AssertionError ae) {
			System.out.println("Don't worry...Expected to fail for \"my-planet-1\" CDK_DEFAULT_REGION with message: "+ ae.getMessage());
		}
		
		System.setProperty("CDK_DEFAULT_ACCOUNT", ENV_ACCOUNT);
		System.setProperty("CDK_DEFAULT_REGION", ENV_REGION);
		Environment envTestEnvironment = AbstractCdkBuildingBlock.makeEnv(null, null);
		assertNotNull(testEnvironment, "Expected non-null CDK env");
		
		assertEquals(ENV_REGION, envTestEnvironment.getRegion());
		assertEquals(ENV_ACCOUNT, envTestEnvironment.getAccount());

	}
	
	@Test
	void testExtraction() {
		// build test paths
		String GLUE_FILE_NAME = String.format("%s.glue.csv", TOPIC_NAME);
		String TEST_S3_PATH = String.format("%s%s", TOPICS_FOLDER_WITH_DELIMETER, GLUE_FILE_NAME);
		
		System.out.println("Testing S3 path: "+ TEST_S3_PATH);
		
		String extractedGlueFilename = AbstractCdkBuildingBlock.extractGlueSchemaFile(TEST_S3_PATH);
		assertEquals(GLUE_FILE_NAME, extractedGlueFilename, "Extraction of Glue Schema file name must match");
		
		String extractedKafkaTopicname = AbstractCdkBuildingBlock.extractKafkaTopic(GLUE_FILE_NAME);
		assertEquals(TOPIC_NAME, extractedKafkaTopicname, "Extraction of Kafka Topic name must match");
	}
	
	@Test
	void testGetBaseFolder() {
		final boolean ERROR_FOLDER = true;
		
		// Landing
		final String landingTopic = "tcs__agency_address";
		String landingBaseFolder = AbstractCdkBuildingBlock.getBaseFolder(TEST_MDOULE_NAME, (! ERROR_FOLDER), landingTopic);
		assertNotNull(landingBaseFolder);
		assertFalse(landingBaseFolder.contains(TEST_MDOULE_NAME), "Landing s3 folder path must NOT include module_name: "+ TEST_MDOULE_NAME);
		assertEquals("landing-topics/"+ landingTopic + "/", landingBaseFolder);
		
		String landingBaseErrorFolder = AbstractCdkBuildingBlock.getBaseFolder(TEST_MDOULE_NAME, ERROR_FOLDER, landingTopic);
		assertNotNull(landingBaseErrorFolder);
		assertFalse(landingBaseErrorFolder.contains(TEST_MDOULE_NAME), "Landing s3 Error folder path must NOT include module_name: "+ TEST_MDOULE_NAME);
		assertEquals("landing-topics/error_records/"+ landingTopic + "/", landingBaseErrorFolder);
		
		// Conformance
		final String conformanceTopic = "conformance_target_concust__agcy";
		String conformanceBaseFolder = AbstractCdkBuildingBlock.getBaseFolder(TEST_MDOULE_NAME, (! ERROR_FOLDER), conformanceTopic);
		assertNotNull(conformanceBaseFolder);
		assertTrue(conformanceBaseFolder.contains(TEST_MDOULE_NAME), "Conformance s3 folder path MUST include module_name: "+ TEST_MDOULE_NAME);
		assertEquals("conformance-topics/"+ TEST_MDOULE_NAME + "/" + conformanceTopic + "/", conformanceBaseFolder);
		
		String conformanceBaseErrorFolder = AbstractCdkBuildingBlock.getBaseFolder(TEST_MDOULE_NAME, ERROR_FOLDER, conformanceTopic);
		assertNotNull(conformanceBaseErrorFolder);
		assertTrue(conformanceBaseErrorFolder.contains(TEST_MDOULE_NAME), "Conformance s3 Error folder path MUST include module_name: "+ TEST_MDOULE_NAME);
		assertEquals("conformance-topics/error_records/"+ TEST_MDOULE_NAME + "/" + conformanceTopic + "/", conformanceBaseErrorFolder);
	}

}
