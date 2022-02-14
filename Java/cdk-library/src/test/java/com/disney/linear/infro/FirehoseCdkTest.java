package com.disney.linear.infro;

import static com.disney.linear.infra.common.CdkConstants.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import com.disney.linear.infra.FirehoseCdk;

/**
 * 
 * @author chawl001
 */
class FirehoseCdkTest {
	
	static final String TEST_MDOULE_NAME = "con-test";

	@Test
	static void testGetDestinationFolder() {
		// Landing
		final String landingTopic = "tcs__agency_address";
		String destFolderLanding = FirehoseCdk.getDestinationFolder(TEST_MDOULE_NAME, landingTopic);
		assertEquals(
				String.format("%s/%s/", BASE_SUCCESS_FOLDER_LANDING, landingTopic) + DATE_PARTITION_PATTERN, 
				destFolderLanding, 
				"Destination folder must match for Landing"
		);
		
		// Conformance
		final String conformanceTopic = "conformance_target_concust__agcy";
		String destFolderConformance = FirehoseCdk.getDestinationFolder(TEST_MDOULE_NAME, conformanceTopic);
		assertEquals(
				String.format("%s/%s/%s", BASE_SUCCESS_FOLDER_CONFORMANCE, TEST_MDOULE_NAME, conformanceTopic) + DATE_PARTITION_PATTERN, 
				destFolderConformance, 
				"Destination folder must match for Conformance"
		);
	}

	@Test
	static void testGetErrorFolder() {
		// Landing
		final String landingTopic = "tcs__agency_address";
		String errFolderLanding = FirehoseCdk.getDestinationFolder(TEST_MDOULE_NAME, landingTopic);
		assertEquals(
				String.format("%s/%s/%s/", BASE_SUCCESS_FOLDER_LANDING, ERROR_FOLDER, landingTopic) + DATE_PARTITION_PATTERN, 
				errFolderLanding, 
				"Error folder must match for Landing"
		);
		
		// Conformance
		final String conformanceTopic = "conformance_target_concust__agcy";
		String errFolderConformance = FirehoseCdk.getDestinationFolder(TEST_MDOULE_NAME, conformanceTopic);
		assertEquals(
				String.format("%s/%s/%s/%s/%s/%s", BASE_SUCCESS_FOLDER_CONFORMANCE, ERROR_FOLDER, TEST_MDOULE_NAME, conformanceTopic, ERROR_TYPE_PATTERN, DATE_PARTITION_PATTERN), 
				errFolderConformance,
				"Error folder must match for Conformance"
		);
				
	}
	
}
