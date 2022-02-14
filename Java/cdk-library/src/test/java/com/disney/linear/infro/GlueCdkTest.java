package com.disney.linear.infro;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.List;
import java.util.ListIterator;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.disney.linear.infra.GlueCdk;
import com.opencsv.exceptions.CsvValidationException;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.utils.Pair;

/**
 * 
 * @author chawl001
 */
class GlueCdkTest {

	String TEST_S3_BUCKET = "laap-ue1-gen-repository-sbx";
	String TEST_TOPICS_FOLDER_WITH_DELIMETER = "avro_2_glue/output/cdk_test/";
	
	String TEST_LOCAL_SCHEMA_FOLDER = "src/main/resources/cdk_test/";
	String TEST_GLUE_SCHEMA_FILENAME = "conformance_target_concommon__flght_rng.glue.csv";
	
	String TOPIC_NAME = 
//			"lndcdc_ncs_tcs__flight_dates" 	/* -- GoldenGate topics */ 
			"lndcdcncstcs__flightdates" 	/* -- NiFi topics */ ;
	
    static final S3Client s3 = S3Client.builder()
            .region(Region.US_EAST_1)
            .build();
	
	@Test
	@Tag("integration-test")	// Needs file on real S3 with permissions setup
	void testGetGlueSchemaS3() throws CsvValidationException, IOException {

    	ListObjectsV2Request listObjects = ListObjectsV2Request
                .builder()
                .bucket(TEST_S3_BUCKET)
                .prefix(TEST_TOPICS_FOLDER_WITH_DELIMETER)
                .build();

        ListObjectsV2Response res = s3.listObjectsV2(listObjects);
        List<S3Object> objects = res.contents();
        
        assertNotNull(objects);
        assertFalse(objects.isEmpty(), String.format("S3 folder s3://%s/%s has no Glue schema files", TEST_S3_BUCKET, TEST_TOPICS_FOLDER_WITH_DELIMETER));
        System.out.println("S3 folder path: "+ TEST_TOPICS_FOLDER_WITH_DELIMETER +" has total "+ objects.size() +" glue schemas");

        ListIterator<S3Object> iterVals = objects.listIterator();
        if (iterVals.hasNext()) {
        	S3Object glueSchemaCSV = iterVals.next();
        	
            System.out.println("Testing parsing of Glue Schema (CSV): " + glueSchemaCSV.key());
            List<Pair<String,String>> columnsList = null;
            try {
            	columnsList = GlueCdk.getGlueSchemaFromS3(s3, TEST_S3_BUCKET, glueSchemaCSV.key());
            } catch (RuntimeException e) {
            	e.printStackTrace();
            	fail("Failed with Exception: "+ e.getMessage());
            }
            assertNotNull(columnsList);
            assertFalse(columnsList.isEmpty());
            for (Pair<String,String> colInfo : columnsList) {
            	System.out.println(colInfo);
            }
         }
	}
	
	@Test
	@Tag("integration-test")	// Needs file on local folder
	void testGetGlueSchemaLocal() throws CsvValidationException, IOException {


        List<Pair<String,String>> columnsList = null;
        try {
        	columnsList = GlueCdk.getGlueSchemaFromFolder(TEST_LOCAL_SCHEMA_FOLDER, TEST_GLUE_SCHEMA_FILENAME);
        } catch (RuntimeException e) {
        	e.printStackTrace();
        	fail("Failed with Exception: "+ e.getMessage());
        }
        assertNotNull(columnsList);
        assertFalse(columnsList.isEmpty());
        for (Pair<String,String> colInfo : columnsList) {
        	System.out.println(colInfo);
        }
	}

}
