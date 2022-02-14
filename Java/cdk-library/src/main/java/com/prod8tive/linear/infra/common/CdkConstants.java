package com.disney.linear.infra.common;

import software.amazon.awssdk.regions.Region;

public interface CdkConstants {
	public static final Region DEFAULT_REGION 		= Region.US_EAST_1;
	public static final String DEFAULT_QUALIFIER 	= "datasync07";		// Refer: cdk.json
	public static final String DEFAULT_ENV 			= "sbx";
	public static final String DEFAULT_ZONE			= "landing";
	
	public static final String CDK_PROPERTY_QUALIFIER 		= "CDK_CONTEXT_QUALIFIER";
	public static final String CDK_PROPERTY_ENV_NAME 		= "CDK_CONTEXT_ENV_NAME";
	public static final String CDK_PROPERTY_MODULE_NAME 	= "CDK_CONTEXT_MODULE_NAME";
	public static final String CDK_PROPERTY_ZONE_NAME		= "CDK_CONTEXT_ZONE_NAME";
	
	public static final String CONTEXT_KEY_QUALIFIER		= "qualifier";
	public static final String CONTEXT_KEY_AWS_ENV_NAME 	= "aws_env_name";
	public static final String CONTEXT_KEY_MODULE_NAME 		= "module_name";
	public static final String CONTEXT_KEY_ZONE_NAME 		= "zone_name";
	
	public static final String GLUE_DATABASE_PATTERN__LANDING		= "laap_glue_db_%s_%s_%s";
	public static final String GLUE_DATABASE_PATTERN__CONFORMANCE	= "laap_glue_db_%s_%s_%s_%s";
	
	public static final String BASE_SUCCESS_FOLDER_LANDING 		= "landing-topics/parquet-test";
	public static final String BASE_SUCCESS_FOLDER_CONFORMANCE 	= "conformance-topics/parquet-test";
	
	public static final int LAMBDA_TRIGGER_LIMIT_100 = 100;
	public static final int LAMBDA_TRIGGER_LIMIT_500 = 500;
	
	public static final String ERROR_FOLDER	= "error_records";
	
	public static final String DATE_PARTITION_PATTERN = "year=!{timestamp:YYYY}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/";
	public static final String ERROR_TYPE_PATTERN 	= "!{firehose:error-output-type}";
	
	public static final String SERIALIZATION_FORMAT = "1";
	public static final String COMPRESSION_FORMAT_SNAPPY = "SNAPPY";
	
	public static final int FIREHOSE_NAME_ID_MAX_LENGTH = 64;
	
	public static final String TAG_USER 		= "user";
	public static final String TAG_USAGE 		= "usage";
	public static final String TAG_ZONE 		= "zone";
	public static final String TAG_CDK_QUALIFIER= "cdk_qualifier";
	public static final String TAG_CDK_ENV 		= "cdk_env";
	public static final String TAG_MODULE 		= "module";
	public static final String TAG_TOPIC		= "topic_name";
	public static final String TAG_CDK_CLASS 	= "cdk_stack_class";

	public static final String LAMBDA_ROLE_PREFIX 	= "laap-role-data-sink-lambda-execution-%s"; // <env>
	public static final String LAMBDA_POLICY_STATEMENT_PREFIX 	= "laap-pol-data-sink-lambda-execution-";
	
	public static final String CDK_ID_CFN_OUTPUT_QUALIFIER = "cfn-output-%s-%s-%s"; // append cdk_qualifier, module_name & zone_name at runtime
	
	enum Format {
		
		AVRO("avro",
			"org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
			"org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
			"org.apache.hadoop.hive.serde2.avro.AvroSerDe"
		),
		PARQUET("parquet",
			"org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
			"org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
			"org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
		);
		
		private String description;
		private String inputFormat;
		private String outputFormat;
		private String serDe;
		
		private Format(String formatName, String inputFormat, String outputFormat, String serDe) {
			this.description = formatName;
			this.inputFormat = inputFormat;
			this.outputFormat = outputFormat;
			this.serDe = serDe;
		}
		
		public String description() {
			return this.description;
		}
		public String inputFormatInfo() {
			return this.inputFormat;
		}
		public String outputFormatInfo() {
			return this.outputFormat;
		}
		public String serDeInfo() {
			return this.serDe;
		}
	}
}
