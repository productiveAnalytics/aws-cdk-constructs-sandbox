package com.disney.linear.infra;

import static com.disney.linear.infra.common.CdkConstants.COMPRESSION_FORMAT_SNAPPY;
import static com.disney.linear.infra.common.CdkConstants.BASE_SUCCESS_FOLDER_LANDING;
import static com.disney.linear.infra.common.CdkConstants.BASE_SUCCESS_FOLDER_CONFORMANCE;
import static com.disney.linear.infra.common.CdkConstants.DATE_PARTITION_PATTERN;
import static com.disney.linear.infra.common.CdkConstants.ERROR_TYPE_PATTERN;
import static com.disney.linear.infra.common.CdkConstants.ERROR_FOLDER;

import java.util.Iterator;
import java.util.Objects;

import com.disney.linear.infra.common.AbstractCdkBuildingBlock;
import com.disney.linear.infra.common.FolderTraverser;
import com.disney.linear.infra.common.ModuleEnvEnabledStackProps;

import software.constructs.Construct;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.events.targets.CloudWatchLogGroup;
import software.amazon.awscdk.services.iam.IGrantable;
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream;
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream.BufferingHintsProperty;
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream.DataFormatConversionConfigurationProperty;
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream.DeserializerProperty;
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream.EncryptionConfigurationProperty;
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream.ExtendedS3DestinationConfigurationProperty;
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream.InputFormatConfigurationProperty;
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream.KMSEncryptionConfigProperty;
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream.OpenXJsonSerDeProperty;
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream.OutputFormatConfigurationProperty;
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream.ParquetSerDeProperty;
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream.SchemaConfigurationProperty;
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream.SerializerProperty;
import software.amazon.awscdk.services.kinesisfirehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty;
import software.amazon.awscdk.services.kms.IAlias;
import software.amazon.awscdk.services.kms.Alias;
import software.amazon.awscdk.services.kms.IKey;
import software.amazon.awscdk.services.kms.Key;
import software.amazon.awscdk.services.kms.KeyLookupOptions;
import software.amazon.awscdk.services.logs.ILogGroup;
import software.amazon.awscdk.services.logs.LogGroup;
import software.amazon.awscdk.services.logs.RetentionDays;
import software.amazon.awscdk.services.logs.StreamOptions;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.Pair;

/**
 * @author chawl001
 *
 */
public class FirehoseCdk extends AbstractCdkBuildingBlock {
	private static final String KMS_KEY_ALIAS 	= "laap-kms-ue1-%s-%s";
	
	private static final Number MINS_1 		= new Integer(60);
	private static final Number MINS_5 		= new Integer(300);
	private static final Number SECONDS_90 	= new Integer(90);
	private static final Number MBS_128 = new Integer(128);
	
	private static final BufferingHintsProperty s3BufferProperty = BufferingHintsProperty.builder()
			.intervalInSeconds(MINS_1)
			.sizeInMBs(MBS_128)
			.build();
	
	private static final DeserializerProperty DESERIALIZER_XJSON = DeserializerProperty.builder()
			.openXJsonSerDe(OpenXJsonSerDeProperty.builder()
					.caseInsensitive(true)
					.build()
			)
			.build();
	private static final InputFormatConfigurationProperty INPUT_FORMAT_CFG = InputFormatConfigurationProperty.builder()
			.deserializer(DESERIALIZER_XJSON)
			.build();
	
	private static final SerializerProperty SERIALIZER_PARQUET = SerializerProperty.builder()
			.parquetSerDe(ParquetSerDeProperty.builder()
					.compression(COMPRESSION_FORMAT_SNAPPY)
					.build()
			)
			.build();
	private static final OutputFormatConfigurationProperty OUTPUT_FORMAT_CFG = OutputFormatConfigurationProperty.builder()
			.serializer(SERIALIZER_PARQUET)
			.build();
	
//	private String landingKMSKeyArn;
//	private String conformanceKMSKeyArn;

	public FirehoseCdk(final Construct parent, final String id) {
		this(parent, id, null);
	}

	public FirehoseCdk(Construct parent, String id, /* final ModuleEnvEnabledStackProps moduleEnvStackProps */ final StackProps moduleEnvStackProps) {
        super(parent, id, moduleEnvStackProps);
		
        final String TOPICS_FOLDER = getTopicsFolder();
        
//        this.landingKMSKeyArn 		= getKMSKeyArn(true);
//        this.conformanceKMSKeyArn 	= getKMSKeyArn(false);
        
        

        try {
            String schemaFilePath;
            String kafkaTopicname;
            CfnDeliveryStream kdf;
            
    		String CW_GROUP_NAME = String.format("/aws/kinesisfirehose/laap-%s-%s-%s", getCdkQualifier(), getModuleName(), getZoneName());
    		String CW_GROUP_ID 	 = String.format("cw-log-grp-%s-%s-%s", getCdkQualifier(), hyphenate(getModuleName()),  getZoneName());
    		
    		// TODO: move log-group creation to common
//    		ILogGroup logGroup = LogGroup.Builder.create(this, CW_GROUP_ID)
//    								.logGroupName(CW_GROUP_NAME)
//    								.retention(RetentionDays.TWO_WEEKS)
//    								.build();
    		ILogGroup logGroup = LogGroup.fromLogGroupName(this, CW_GROUP_ID, CW_GROUP_NAME);

            FolderTraverser<Pair<String,String>> topicsFolderTraverser = new FolderTraverser<>();
      	  	for (Iterator<Pair<String,String>> iterPairs = topicsFolderTraverser.iterator(); iterPairs.hasNext(); ) {
      	  		schemaFilePath = iterPairs.next().right();
                System.out.println("The name of the file is " + schemaFilePath);
                
                kafkaTopicname = extractKafkaTopic(extractGlueSchemaFile(schemaFilePath));
                System.out.println("Referring to Glue Table: "+ kafkaTopicname);
                
                kdf = createKDF(parent, kafkaTopicname, logGroup);
                System.out.println("Created KDF: "+ kdf.getLogicalId());
             }

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
	}
	
    private String getKMSKeyArn(final boolean isLanding) {
    	
    	String zone = getZoneName(isLanding);
    	String kmsKeyAlias = String.format(KMS_KEY_ALIAS, zone, getCdkEnvName());
    	Objects.requireNonNull(kmsKeyAlias);
    	
    	String KMS_KEY_ALIAS_ID = "kms-key-alias-"+ kmsKeyAlias;
    	IAlias kmsKeyAliasObj = Alias.fromAliasName(this, KMS_KEY_ALIAS_ID, kmsKeyAlias);
    
    	Objects.requireNonNull(kmsKeyAliasObj, "ERROR: Cannot get ARN for KMS Key Alias: "+ kmsKeyAlias);
    	String kmsKeyArn = kmsKeyAliasObj.getKeyArn();
    	System.err.println("KMS ARN (Orig) : "+ kmsKeyArn);
    	kmsKeyArn = "arn:aws:kms:us-east-1:887847050650:key/8d3c12e4-0987-491d-a8c1-338a4a49dbcc";
    	System.err.println("KMS ARN (Landing) : "+ kmsKeyArn);
    	
//    	IKey lookupKmsKey = Key.fromLookup(this, KMS_KEY_ALIAS_ID, KeyLookupOptions.builder().aliasName(kmsKeyAlias).build());
//    	Objects.requireNonNull(lookupKmsKey);
//    	String kmsKeyArn = lookupKmsKey.getKeyArn();
    	
    	return kmsKeyArn;
    }
    
    public static String getDestinationFolder(final String moduleName, final String kafkaTopic) {
    	final boolean errorFolderFlag = false;
    	String successFolderPath = getBaseFolder(moduleName, errorFolderFlag, kafkaTopic) + DATE_PARTITION_PATTERN;
    	return successFolderPath;
    }
    
    public static String getErrorFolder(final String moduleName, String kafkaTopic){
    	final boolean errorFolderFlag = true;
    	String baseErrorFolderPath = getBaseFolder(moduleName, errorFolderFlag, kafkaTopic);
    	String errorFolderPath = String.format("%s/%s/%s", baseErrorFolderPath,  ERROR_TYPE_PATTERN, DATE_PARTITION_PATTERN);
    	return errorFolderPath;
    }

	private CfnDeliveryStream createKDF(final Construct parent, final String topicName, final ILogGroup cwLogGroup) {
		final String S3_ARN = getDestinationS3ARN(topicName);
		final String ROLE_ARN = getRoleARN();
		
		final String MODULE_NAME = getModuleName();
		final String GLUE_CATALOG_ID = getAccount();
		
		final boolean isLanding = isLanding(topicName);
        final String GLUE_DATABASE_ID = isLanding ? this.getDatabaseIdLanding() : this.getDatabaseIdConformance();
        
        // Glue Table name = Kafka Topic name
        final String GLUE_TABLE_NAME = topicName;
        final String DEST_FOLDER = getDestinationFolder(MODULE_NAME, topicName);
        final String ERR_FOLDER = getErrorFolder(MODULE_NAME, topicName);
//        final String KMS_KEY_ARN = isLanding(topicName) ? this.landingKMSKeyArn : this.conformanceKMSKeyArn;

		SchemaConfigurationProperty schemaCfgProperty = SchemaConfigurationProperty.builder()
				.region(getRegion())
				.roleArn(ROLE_ARN)
				.catalogId(GLUE_CATALOG_ID)
				.databaseName(GLUE_DATABASE_ID)
				.tableName(GLUE_TABLE_NAME)
				.versionId("LATEST")
				.build();

		final DataFormatConversionConfigurationProperty DATA_FORMAT_CONV_CFG = DataFormatConversionConfigurationProperty.builder()
				.enabled(true)
				.inputFormatConfiguration(INPUT_FORMAT_CFG)
				.outputFormatConfiguration(OUTPUT_FORMAT_CFG)
				.schemaConfiguration(schemaCfgProperty)
				.build();
		
//		final EncryptionConfigurationProperty KMS_ENCRYPTION_CFG = EncryptionConfigurationProperty.builder()
//				.kmsEncryptionConfig(
//						KMSEncryptionConfigProperty.builder()
//						.awskmsKeyArn(KMS_KEY_ARN)
//						.build()
//				)
//				.build();
		
		String zone = getZone(GLUE_TABLE_NAME);
		
		final String FIREHOSE_NAME = getFirehoseName(topicName);
//		cwLogGroup.addStream(FIREHOSE_NAME, 
//				StreamOptions.builder()
//					.logStreamName(FIREHOSE_NAME)
//					.build()
//		);
		
		final CloudWatchLoggingOptionsProperty cwLogProperty = CloudWatchLoggingOptionsProperty.builder()
				.logGroupName(cwLogGroup.getLogGroupName())
				.logStreamName(FIREHOSE_NAME)
				.enabled(true)
				.build();
		
		ExtendedS3DestinationConfigurationProperty extendedS3Property = ExtendedS3DestinationConfigurationProperty.builder()
				.bucketArn(S3_ARN)
				.prefix(DEST_FOLDER)
				.errorOutputPrefix(ERR_FOLDER)
				.roleArn(ROLE_ARN)
				.bufferingHints(s3BufferProperty)
//				.compressionFormat("SNAPPY")
				.dataFormatConversionConfiguration(DATA_FORMAT_CONV_CFG)
//				.encryptionConfiguration(KMS_ENCRYPTION_CFG)
				.cloudWatchLoggingOptions(cwLogProperty)
				.build();
		
		final String FIREHOSE_CDK_ID = getFirehoseCdkId(topicName);
		
		CfnDeliveryStream firehose = CfnDeliveryStream.Builder
				.create(this, FIREHOSE_CDK_ID)
				.deliveryStreamName(FIREHOSE_NAME) 
				.deliveryStreamType("DirectPut")
				.extendedS3DestinationConfiguration(extendedS3Property)
				.tags(getCfnTags(topicName))
				.build();
		
//		String FIREHOSE_EXPORT_VALUE = firehose.getAttrArn();
//		this.exportValue(FIREHOSE_EXPORT_VALUE, ExportValueOptions.builder().name("KDF-"+ cdkSafeId(topicName)).build());
//		String exportedFirehoseName = String.format("firehose-%s", hyphenate(topicName));
//        new CfnOutput(this,
//        		"cnf-output-"+ FIREHOSE_CDK_ID,
//        		CfnOutputProps.builder()
//        			.exportName(exportedFirehoseName)
//        			.description("Firehose for topic: "+ topicName)
//        			.value(FIREHOSE_EXPORT_VALUE)
//        			.build()
//        );
		
        return firehose;
	}

}
