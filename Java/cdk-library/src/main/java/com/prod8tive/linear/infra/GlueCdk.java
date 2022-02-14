package com.disney.linear.infra;

import static com.disney.linear.infra.common.CdkConstants.CDK_ID_CFN_OUTPUT_QUALIFIER;
import static com.disney.linear.infra.common.CdkConstants.SERIALIZATION_FORMAT;
import static com.disney.linear.infra.common.CdkConstants.Format;
import static com.disney.linear.infra.common.CdkConstants.Format.AVRO;
import static com.disney.linear.infra.common.CdkConstants.Format.PARQUET;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.io.IOException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

import java.time.Instant;

import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.CfnOutputProps;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.glue.CfnDatabase;
import software.amazon.awscdk.services.glue.CfnDatabase.DatabaseInputProperty;
import software.amazon.awscdk.services.glue.CfnTable;
import software.amazon.awscdk.services.glue.CfnTable.ColumnProperty;
import software.amazon.awscdk.services.glue.CfnTable.SerdeInfoProperty;
import software.amazon.awscdk.services.glue.CfnTable.StorageDescriptorProperty;
import software.amazon.awscdk.services.glue.CfnTable.TableInputProperty;

import software.constructs.Construct;

import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.utils.Pair;

import com.disney.linear.infra.common.AbstractCdkBuildingBlock;
import com.disney.linear.infra.common.FolderTraverser;
import com.opencsv.CSVIterator;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

/**
 * @author chawl001
 *
 */
public class GlueCdk extends AbstractCdkBuildingBlock {
	private static final String LOCAL_SCHEMA_FOLDER = "./src/main/resources/cdk_test/";
	private Format dataFormat;
	
    public GlueCdk(final Construct parent, final String id) {
        this(parent, id, null, false);
    }

    public GlueCdk(final Construct parent, final String id, final StackProps moduleEnvStackProps, final boolean useParquet) {
        super(parent, id, moduleEnvStackProps);
        
        dataFormat = useParquet ? PARQUET : AVRO;
        
        System.err.println("CDK Account==="+ getAccount());
        System.err.println("CDK Region==="+ getRegion());
        
        final String MODULE_NAME = getModuleName();
        final String ZONE_NAME	 = getZoneName();
        
        final String GLUE_CATALOG_ID = getCatalogId();
        System.out.println("AWS A/C Id (as Catalog Id): "+ GLUE_CATALOG_ID);
        
        final String GLUE_DATABASE_ID = getDatabaseIdConformance();
        
        DatabaseInputProperty dbInputProperty = DatabaseInputProperty.builder()
        		.name(GLUE_DATABASE_ID)
        		.description("CDK Glue db: "+ GLUE_DATABASE_ID)
        		.locationUri("URL for "+ GLUE_DATABASE_ID)
        		.build();

        CfnDatabase cdkGlueDB = null;
//        if (isExistingGlueDb()) {
//        	System.out.println("Using existing Glue DB: "+ getDatabaseId());
//        } 
//        else
//        {
        	/*
        	final String GLUE_DATABASE_CDK_ID = "linear_cdk_glue_db_"+ getModuleName() +"_"+ getZoneName();
        	
	        cdkGlueDB = CfnDatabase.Builder
	        		.create(this, GLUE_DATABASE_CDK_ID)
	        		.catalogId(GLUE_CATALOG_ID)
	        		.databaseInput(dbInputProperty)
	        		.build();
	        
	        String _MODULE_NAME = hyphenate(MODULE_NAME);
	        String exportedGlueDbName = String.format("%s-%s-%s-glue-db", getCdkQualifier(), _MODULE_NAME, ZONE_NAME);
	        String EXPORTED_GLUE_DB_CDK_ID = String.format(CDK_ID_CFN_OUTPUT_QUALIFIER, getCdkQualifier(), _MODULE_NAME, ZONE_NAME) + "-glue-db";
	        new CfnOutput(this,
	        		EXPORTED_GLUE_DB_CDK_ID,
	        		CfnOutputProps.builder()
	        			.exportName(exportedGlueDbName)
	        			.description("Glue Database for module "+ MODULE_NAME +" created by CDK qualifier: "+ getCdkQualifier())
	        			.value(GLUE_DATABASE_ID)
	        			.build()
	        );
	        
	        System.out.println("Created Glue DB: "+ cdkGlueDB.getStack());
	        */
//        }
        
        final String TOPICS_FOLDER = getTopicsFolder();
        System.err.println("CDK Topics Folder==="+ TOPICS_FOLDER);
        
        final S3Client s3 = AbstractCdkBuildingBlock.getS3Client(/*getRegion()*/ "us-east-1", true);
        String schemaFilePath;
        try {
        	  FolderTraverser<Pair<String,String>> topicsFolderTraverser = new FolderTraverser<>();
        	  for (Iterator<Pair<String,String>> iterPairs = topicsFolderTraverser.iterator(); iterPairs.hasNext(); ) {
        		  schemaFilePath = iterPairs.next().right();
        		  System.out.print("\n The name of the file is " + schemaFilePath);
                
	                try {
	                	CfnTable cfnTable = createGlueTable(s3, schemaFilePath);
		                
		                // Must wait for Glue DB creation before attempting creation of new Glue table
//		                if (!isExistingGlueDb() && null != cdkGlueDB) {
	                	if (getDatabaseIdConformance().equals(cfnTable.getDatabaseName()) && null != cdkGlueDB) {
		                	cfnTable.getNode().addDependency(cdkGlueDB);
		                }
		                
		                System.out.println("Created Glue Table : "+ cfnTable.getLogicalId());
	                } catch (IOException | CsvValidationException runtimeEx) {
	                	throw new RuntimeException(runtimeEx.getMessage());
	                }
             }

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
    
    private CfnTable createGlueTable(final S3Client s3Cli, final String schemaFilePath) 
    		throws IOException, CsvValidationException
    {
    	final String MODULE_NAME = getModuleName();
    	final String _MODULE_NAME = hyphenate(getModuleName());
        final String GLUE_CATALOG_ID = getCatalogId();
    	
    	// Glue Table name matches Kafka Topic name
        final String KAFKA_TOPIC = extractKafkaTopic(schemaFilePath);
        final boolean isLanding = isLanding(KAFKA_TOPIC);
        final String GLUE_DATABASE_ID = isLanding ? this.getDatabaseIdLanding() : this.getDatabaseIdConformance();
    	
        final String S3_DESTINATION_FOLDER_PATH = getBaseFolder(MODULE_NAME, false, KAFKA_TOPIC);
    	final String S3_LOCATION = "s3://"+ getDestinationS3Bucket(KAFKA_TOPIC) +"/"+ S3_DESTINATION_FOLDER_PATH;
    	
        Map<String, String> serdeParamMap = new HashMap<>();
        serdeParamMap.put("serialization.format", SERIALIZATION_FORMAT);
        
        SerdeInfoProperty serdeProperty = SerdeInfoProperty.builder()
        		.name(dataFormat.serDeInfo())
        		.parameters(serdeParamMap)
        		.build();
        
        Map<String, String> tableInputParamMap = new HashMap<>();
        tableInputParamMap.put("connectionName", "JARVIS Kafka");
        tableInputParamMap.put("classification", dataFormat.name().toLowerCase());
    	
//        List<Pair<String,String>> columnsList = getGlueSchema(s3Cli, getS3Bucket(), schemaFilePath);
        List<Pair<String,String>> columnsList = getGlueSchemaFromFolder(LOCAL_SCHEMA_FOLDER, schemaFilePath);
        
        List<ColumnProperty> columns = new ArrayList<>();
        ColumnProperty columnProperty = null;
        String columnName = null;
        
        for (Pair<String,String> columnInfo : columnsList) {
        	columnName = columnInfo.left();
            columnProperty = ColumnProperty.builder()
                    .name(columnName)
                    .comment("comment for column: "+ columnName.toUpperCase())
                    .type(columnInfo.right())
                    .build();
            columns.add(columnProperty);
        }
        
        Map<String, String> storageDescParamMap = new HashMap<>();
        storageDescParamMap.put("connectionName", "JARVIS Kafka");
        storageDescParamMap.put("typeOfData", "kafka");
        storageDescParamMap.put("topicName", KAFKA_TOPIC);
        
        TableInputProperty tblInputProps = TableInputProperty.builder()
        		.name(KAFKA_TOPIC)
        		.description("Glue Table with CDK")
        		.storageDescriptor(StorageDescriptorProperty.builder()
        					.columns(columns)
        					.location(S3_LOCATION)
        					.inputFormat(dataFormat.inputFormatInfo())
        					.outputFormat(dataFormat.outputFormatInfo())
        					.serdeInfo(serdeProperty)
        					.parameters(storageDescParamMap)
        					.build()
        				)
        		.partitionKeys(PARTITION_COLUMNS)
        		.tableType("EXTERNAL_TABLE")
        		.parameters(tableInputParamMap)
        		.build();
        
        String GLUE_TABLE_CDK_ID = String.format("%s-%s-%s-table", getCdkQualifier(), _MODULE_NAME, KAFKA_TOPIC);
        CfnTable cfnTable = CfnTable.Builder
        		.create(this, GLUE_TABLE_CDK_ID)
        		.catalogId(GLUE_CATALOG_ID)
        		.databaseName(GLUE_DATABASE_ID)
        		.tableInput(tblInputProps)
        		.build();
        
        String glueTableExportValue = GLUE_DATABASE_ID +"."+ KAFKA_TOPIC;
        
        // show the output
//        this.exportValue(glueTableExportValue, ExportValueOptions.builder().name("GlueTable-"+ Instant.now().toEpochMilli()).build());
        String exportedTableName = String.format("glue-table-%s", hyphenate(KAFKA_TOPIC));
//        new CfnOutput(this,
//        		"cnf-output-"+ GLUE_TABLE_CDK_ID,
//        		CfnOutputProps.builder()
//        			.exportName(exportedTableName)
//        			.description("Glue Table for topic: "+ KAFKA_TOPIC)
//        			.value(glueTableExportValue)
//        			.build()
//        );
        
        return cfnTable;
    }
    
    // TODO: Pass TOKEN-resolved S3 token (currently tested with static bucket-name)
    public static List<Pair<String,String>> getGlueSchemaFromS3(final S3Client s3Cli, final String s3Bucket, final String schemaFileS3Path) 
    		throws IOException, CsvValidationException 
    {
    	GetObjectRequest getObjectReq = GetObjectRequest
        		.builder() 
        		.bucket(s3Bucket)
        		.key(schemaFileS3Path)
        		.build();
    	
    	final String glueSchemaFileName = extractGlueSchemaFile(schemaFileS3Path);
    	
    	long epoch = Instant.now().toEpochMilli();
    	Path timebaseDir = Files.createTempDirectory(String.valueOf(epoch)).toAbsolutePath();
    	Path tempFilePath =  Paths.get(timebaseDir.toString(), glueSchemaFileName+".tmp"); // Files.createTempFile(glueSchemaFileName, ".tmp");
    	GetObjectResponse getObjectRes = s3Cli.getObject(getObjectReq, ResponseTransformer.toFile(tempFilePath));
    	assert (getObjectRes.contentLength() > 0);
    	
    	List<Pair<String,String>> tableColumns = new ArrayList<>();
    	tableColumns = parseSchemaCSV(tempFilePath);
    	
    	return tableColumns;
    }
    
    public static List<Pair<String,String>> getGlueSchemaFromFolder(final String localFolderPath, final String schemaFileS3Path) 
    		throws CsvValidationException, FileNotFoundException, IOException 
    {
    	Path schemaFilePath = Paths.get(localFolderPath, schemaFileS3Path);
    	return parseSchemaCSV(schemaFilePath);
    }
    
    private static List<Pair<String, String>> parseSchemaCSV(Path filePath) 
    		throws FileNotFoundException, IOException, CsvValidationException
    {
    	System.out.println("Checking for file: "+ filePath.toAbsolutePath());
    	System.out.println("Exists? : "+ Files.exists(filePath));
    	System.out.println("Redable? : "+Files.isReadable(filePath));
    	
    	File csvFile = filePath.toFile();
    	
    	try (CSVReader csvReader = new CSVReader(new FileReader(csvFile))) {
    		List<Pair<String,String>> tableColumns = new ArrayList<>();
	    	CSVIterator csvIter = new CSVIterator(csvReader);
	    	Pair<String,String> columnInfo;
	    	String[] csvLine;
	    	while (csvIter.hasNext()) {
	    		csvLine = csvIter.next();
	    		columnInfo = Pair.of(csvLine[0], csvLine[1]);
	    		tableColumns.add(columnInfo);
	    	}

	    	return tableColumns;
    	}
    }
}
