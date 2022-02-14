package com.prod8ctive.infra.common;

import static com.prod8ctive.infra.common.CdkConstants.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.jetbrains.annotations.NotNull;

import software.constructs.Construct;

// CDK v2
import software.amazon.awscdk.CfnParameter;
import software.amazon.awscdk.CfnTag;
import software.amazon.awscdk.Environment;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.Token;
import software.amazon.awscdk.regioninfo.RegionInfo;
import software.amazon.awssdk.utils.StringUtils;

import software.amazon.awscdk.services.glue.CfnTable.ColumnProperty;

// import software.amazon.awscdk.services.s3.assets.*;

// SDK v2
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;


/**
 * 
 * @author chawl001
 * @param <V>
 */
public abstract class AbstractCdkBuildingBlock extends Stack implements ModuleEnvEnabled {
	
	private static final String S3_BUCKET_NAME  = "laap-ue1-gen-%s-%s";
	
	private static final String KDF_CDK_ID 		= "cdk-kdf-%s-%s-%s-%s";
	private static final String KDF_NAME 		= "laap-kdf-ue1-datasink-%s-%s-%s";

	private static final ColumnProperty yearPartition = ColumnProperty.builder()
            .name("year")
            .comment("Partition column: year (e.g. year=yyyy)")
            .type("string")
            .build();
	
	private static final ColumnProperty monthPartition = ColumnProperty.builder()
            .name("month")
            .comment("Partition column: month (e.g. month=MM)")
            .type("string")
            .build();
	
	private static final ColumnProperty dayPartition = ColumnProperty.builder()
            .name("day")
            .comment("Partition column: day (e.g. day=dd)")
            .type("string")
            .build();
	
	private static final ColumnProperty hourPartition = ColumnProperty.builder()
            .name("hour")
            .comment("Partition column: day (e.g. hour=HH)")
            .type("string")
            .build();
	
	protected static final List<ColumnProperty> PARTITION_COLUMNS = Arrays.asList(new ColumnProperty[]
			{
					yearPartition, 
					monthPartition, 
					dayPartition, 
					hourPartition
			}
	);
	
	private CfnParameter role_name;
	private CfnParameter topics_s3_bucket;
	private CfnParameter topics_folder;
	
//	private ModuleEnvEnabledStackProps moduleEnvStackProps;
	private String awsAccountId;
	private String awsRegionId;
	private String cdkQualifier;
	private String cdkModuleName;
	private String cdkZoneName;
	private String cdkEnvName;

	public AbstractCdkBuildingBlock(Construct parent, String id, /* final ModuleEnvEnabledStackProps stackProps */ final StackProps stackProps) {
		super(parent, id, stackProps);
		
		// set module_name and env from CDK context
//		this.moduleEnvStackProps = stackProps;
		this.initialize(stackProps);
		
		role_name = CfnParameter.Builder
				.create(this, "role-name")
				.type("String")
				.description("AWS role name")
				.minLength(3)
				.build();
		topics_s3_bucket = CfnParameter.Builder
				.create(this, "topics-s3-bucket")
				.type("String")
				.description("S3 Bucket name that stores Glue Schema CSV files: typically laap-ue1-gen-repository-sbx")
				.minLength(3)
				.build();
		topics_folder = CfnParameter.Builder
				.create(this, "topics-folder")
				.type("String")
				.description("Folder in S3 for Glue Schema for Topics")
				.minLength(3)
				.build();
	}
	
	private void initialize(final StackProps stackProps) {
		Environment stackEnv = stackProps.getEnv();
		Objects.requireNonNull(stackEnv, "Stack Environment must not be null!");
		this.awsAccountId = stackEnv.getAccount();
		Objects.requireNonNull(awsAccountId, "AWS account id from Stack Environment must not be null!");
		this.awsRegionId  = stackEnv.getRegion();
		Objects.requireNonNull(awsRegionId, "AWS region id from Stack Environment must not be null!");
		
		this.cdkQualifier 	= extractCdkQualifier(this);
		this.cdkEnvName 	= extractCdkEnvName(this);
		this.cdkModuleName 	= extractCdkModuleName(this);
		this.cdkZoneName	= extractCdkZoneName(this);
	}
	
	@Override
	public @NotNull String getAccount() {
		return this.awsAccountId;
	}
	
	/**
	 * Uses CDK region otherwise defaults to "us-east-1"
	 */
	@Override
	public String getRegion() {
		String cdkRegion = this.awsRegionId;
		if (null == cdkRegion || null == RegionInfo.get(cdkRegion)) {
			return DEFAULT_REGION.id();
		} else {
			return cdkRegion;
		}
	}
	
	@Override
	public final @NotNull String getCdkQualifier() {
		return this.cdkQualifier;
	}
	
	@Override
	public final @NotNull String getCdkEnvName() {
		return this.cdkEnvName;
	}
	
	@Override
	public final @NotNull String getModuleName() {
		return this.cdkModuleName;
	}
	
	@Override
	public final @NotNull String getZoneName() {
		return this.cdkZoneName;
	}

	@Override
	public String getDatabaseIdLanding() {
//		return this.moduleEnvStackProps.getDatabaseIdLanding();
		return getDatabaseId(true);
	}

	@Override
	public String getDatabaseIdConformance() {
//		return this.moduleEnvStackProps.getDatabaseIdConformance();
		return getDatabaseId(false);
	}
	
	private String getDatabaseId(final boolean isLanding) {
		String zoneName = AbstractCdkBuildingBlock.getZoneName(isLanding);
		if (isLanding) {
			String landingGlueDb = String.format(GLUE_DATABASE_PATTERN__LANDING,
					this.getCdkQualifier(),
					zoneName, 
					this.getCdkEnvName());
			return landingGlueDb;
		} else {
			String _MODULE = underscore(this.getModuleName());
			String moduleSpecificConformanceGlueDb = String.format(GLUE_DATABASE_PATTERN__CONFORMANCE, 
					this.getCdkQualifier(),
					zoneName, 
					_MODULE, 
					this.getCdkEnvName());
			return moduleSpecificConformanceGlueDb;
		}
	}
	
    
	/**
	 * Common function used by both Role Cdk and Lambda Cdk
	 * @return
	 */
    public final String getRoleId() {
    	String _MODULE_NAME = hyphenate(getModuleName());
//    	final String roleId = String.format("%s-%s-%s", LAMBDA_ROLE_PREFIX, MODULE_NAME, env);
    	String lambdaRoleId = String.format(LAMBDA_ROLE_PREFIX, getCdkQualifier(), _MODULE_NAME, getCdkEnvName());
    	return lambdaRoleId;
    }
	
	/**
	 * Prepare environment with CDK variables CDK_DEFAULT_ACCOUNT, CDK_DEFAULT_REGION
	 * 
	 * @param account
	 * @param region
	 * @return
	 */
    public static Environment makeEnv(final String account, String region) {
        final String cdkAccount = (account == null || StringUtils.isBlank(account)) ? System.getenv("CDK_DEFAULT_ACCOUNT") : account ;
        final String cdkRegion = (region == null || StringUtils.isBlank(region)) ? System.getenv("CDK_DEFAULT_REGION") : region;
        
        Objects.requireNonNull(cdkAccount, "Need valid AWS Account. Hint: export CDK_DEFAULT_ACCOUNT=<aws-account-id>");
        Objects.requireNonNull(cdkRegion, "Need valid AWS Region. Hint: export CDK_DEFAULT_REGION=us-east-1");
        
        final Region lookupRegion = Region.of(cdkRegion);
        System.out.println("Look-up Region==="+ lookupRegion);
        if (! Region.regions().contains(lookupRegion)) {
        	throw new AssertionError(String.format("Received AWS Region: %s. Need valid AWS Region", cdkRegion));
        }

        return Environment.builder()
                .account(cdkAccount)
                .region(cdkRegion)
                .build();
    }
    
    /**
     * 
     * @param bastStackPattern 		Pattern: <app-name>-<module-name>-base_<cdk-stack-class-name>-<env-name>
     * 			e.g. resolves to "datasync-con-common-base_GlueCdk-stack-sbx"
     * @param datasyncEnv
     * @param cdkEnvName
     * @param cdkModuleName
     * @param zoneName
     * @param cdkStackCls
     * @return
     */
//	public static ModuleEnvEnabledStackProps generateBaseStack(final String appName,
//			final Environment environmentObj, 
//			final String cdkEnvName,
//			final String moduleName, 
//			final String zoneName,
//			final Class<? extends AbstractCdkBuildingBlock> cdkStackCls)
//	{
//		final String cdkStackClassName = cdkStackCls.getSimpleName();
//		final String BASE_STACK_NAME = String.format("%s-%s-base-%s-stack-%s", appName, moduleName, cdkStackClassName, cdkEnvName);
//        
//        final Map<String,String> tagsMap = AbstractCdkBuildingBlock.getTagsAsMap(moduleName, cdkEnvName, zoneName, cdkStackCls);
//        
//        final StackProps baseStackProps =  StackProps.builder()
//        		.env(environmentObj)
//        		.stackName(BASE_STACK_NAME)
//        		.terminationProtection(true)
//        		.tags(tagsMap)
//        		.build();
//        
//        final ModuleEnvEnabledStackProps moduleEnvEnabledStackProps =  ModuleEnvEnabledStackProps.builder()
//        		.stackProps(baseStackProps)
//        		.cdkEnvName(cdkEnvName)
//        		.moduleName(moduleName)
//        		.build();
//		return moduleEnvEnabledStackProps;
//	}
    
    public static boolean isLanding(final String topicName) {
    	return topicName.toLowerCase().startsWith("conformance_") ? false : true;
    }
    
    public static String deriveDestinationBucket(final String region, final boolean isLanding, final String envName) {
    	String zoneName = getZoneName(isLanding);
    	String s3Bucket = String.format(S3_BUCKET_NAME, zoneName, envName);
    	return s3Bucket;
    }

    
    public static final Map<String,String> getTagsAsMap(
    		final String qualifier,
    		final String module, 
    		final String cdkEnv, 
    		final String zone, 
    		final Class<? extends AbstractCdkBuildingBlock> cdkStackClass) {

    	Map<String,String> tagsMap = new HashMap<>();
    	tagsMap.put(TAG_USER, "LDC");
    	tagsMap.put(TAG_USAGE, "datasync");
    	tagsMap.put(TAG_CDK_QUALIFIER, qualifier);
    	tagsMap.put(TAG_CDK_ENV, String.valueOf(cdkEnv));
    	tagsMap.put(TAG_MODULE, String.valueOf(module));

    	
    	if (! StringUtils.isBlank(zone)) {
    		tagsMap.put(TAG_ZONE, zone);
    	} else {
    		System.out.println(" [WARN] Skipping tag: "+TAG_ZONE );
    	}
    	
    	final String cdkStackName = (null != cdkStackClass) ? cdkStackClass.getSimpleName() : "Main-App";
    	tagsMap.put(TAG_CDK_CLASS, cdkStackName);
    	
    	return tagsMap;
    }

    protected List<? extends CfnTag> getCfnTags(final String topicName) {
    	String cdk_qualifier= getCdkQualifier();
    	String module_name 	= getModuleName();
    	String cdk_env_name = getCdkEnvName();
    	String zone_name 	= getZone(topicName);
    	
    	Map<String,String> tagsMap = getTagsAsMap(cdk_qualifier, module_name, cdk_env_name, zone_name, this.getClass());
    	
    	tagsMap.put(TAG_TOPIC, topicName);

    	return tagsMap.entrySet().stream()
		    			.map(e -> CfnTag.builder()
							.key(e.getKey())
							.value(e.getValue())
							.build())
		    			.collect(Collectors.toList());
	}
	
	public static final String extractCdkQualifier(final Construct construct) {
		String cdkQualifier = (String) construct.getNode().tryGetContext(CONTEXT_KEY_QUALIFIER);
		Objects.requireNonNull(cdkQualifier, 
				String.format("\n [ERROR] CDK needs --context %s=qualifier \n", CONTEXT_KEY_QUALIFIER)
		);
		
		return cdkQualifier;
	}
	
	public static final String extractCdkEnvName(final Construct construct) {
		String cdkEnv = (String) construct.getNode().tryGetContext(CONTEXT_KEY_AWS_ENV_NAME);
		Objects.requireNonNull(cdkEnv, 
				String.format("\n [WARN] CDK needs --context %s=aws_env_name , where envname in ('sbx', 'dev', 'qa', 'test', 'prod') \n", CONTEXT_KEY_AWS_ENV_NAME)
		);
		return cdkEnv;
	}
	
	public static final String extractCdkModuleName(final Construct construct) {
		String moduleName = (String) construct.getNode().tryGetContext(CONTEXT_KEY_MODULE_NAME);
		Objects.requireNonNull(moduleName, 
				String.format("\n [ERROR] CDK needs --context %s=module_name \n", CONTEXT_KEY_MODULE_NAME)
		);
		
		return moduleName;
	}
	
	public static final String extractCdkZoneName(final Construct construct) {
		String zoneName = (String) construct.getNode().tryGetContext(CONTEXT_KEY_ZONE_NAME);
		Objects.requireNonNull(zoneName, 
				String.format("\n [ERROR] CDK needs --context %s=zone_name \n", CONTEXT_KEY_ZONE_NAME)
		);
		
		return zoneName;
	}
	
	public static String getZone(final String kafkaTopic) {
		return getZoneName(isLanding(kafkaTopic));
	}
	
	public static String getZoneName(final boolean isLanding) {
		return isLanding ? "landing" : "conformance";
	}
	
    protected String getDestinationS3Bucket(final String kafkaTopic) {
    	boolean isLandingS3Bucket = isLanding(kafkaTopic);
    	String destS3Bucket = deriveDestinationBucket(getRegion(), isLandingS3Bucket, getCdkEnvName());
    	Objects.requireNonNull(destS3Bucket, "Destination (Landing/Conformance) bucket must be derivable!");
    	
    	return destS3Bucket;
    }
    
    protected String getDestinationS3ARN(final String kafkaTopic) {
    	return String.format("arn:aws:s3:::%s", getDestinationS3Bucket(kafkaTopic));
    }
    
    protected String getTopicsS3Bucket() {
    	String topicsS3BucketName = topics_s3_bucket.getValueAsString();
    	Objects.requireNonNull(topicsS3BucketName, "CDK needs parameter: topicss3bucket");
    	
    	try {
	    	System.err.println("[DEBUG] s3Bucket="+topicsS3BucketName);
	    	System.err.println("[DEBUG] s3_bucket Token unresolved? "+ Token.isUnresolved(topicsS3BucketName));
	    	
	    	Object resolvedS3Token = this.resolve(topicsS3BucketName);
	    	System.err.println("[DEBUG] Resolved s3 Token: "+ resolvedS3Token.toString());
	    	
	    	// TODO: resolve reference while passing to SDK
	//    	CfnResource cfnRes = this.getNestedStackResource();
	//    	cfnRes.getRef();
    	} catch (Exception e) {
    		// IGNORE
    		e.printStackTrace();
    	}
    	
    	return topicsS3BucketName;
    }
    
    protected String getTopicsFolder() {
    	String topicsFolder = topics_folder.getValueAsString() /* (String) this.getNode().tryGetContext("topics_folder") */;
    	Objects.requireNonNull(topicsFolder, "CDK needs parameter: topicsfolder");
    	
    	System.err.println("[DEBUG] topicsFolder="+topicsFolder);
    	System.err.println("[DEBUG] topics_folder Token unresolved? "+ Token.isUnresolved(topicsFolder));
    	
    	Object resolvedTopicsFolderToken = this.resolve(topicsFolder);
    	System.err.println("Resolved topics_folder Token: "+ resolvedTopicsFolderToken.toString());
    
    	Objects.requireNonNull(topicsFolder, "CDK needs parameter: topicsfolder");
    	
    	return topicsFolder;
    }
    
    public static final String getBaseFolder(final String moduleName, boolean isForError, final String kafkaTopic) {
    	String baseFolderByZone = isLanding(kafkaTopic) ? BASE_SUCCESS_FOLDER_LANDING : BASE_SUCCESS_FOLDER_CONFORMANCE;
    	baseFolderByZone = isForError ? (baseFolderByZone + "/" + ERROR_FOLDER) : baseFolderByZone;
   	
    	String baseFolderPath = null;
    	if (isLanding(kafkaTopic)) {
    		String landingFolder = String.format("%s/%s/", baseFolderByZone, kafkaTopic);
    		baseFolderPath = landingFolder;
    	} else {
    		String conformanceFolder = String.format("%s/%s/%s/", baseFolderByZone, moduleName, kafkaTopic);
    		baseFolderPath = conformanceFolder;
    	}
    	return baseFolderPath;
    }
    
//    public static String getDestinationFolder(final String moduleName, final String kafkaTopic) {
//    	final boolean errorFolderFlag = false;
//    	String destFolderPath = getBaseFolder(moduleName, errorFolderFlag, kafkaTopic) + DATE_PARTITION_PATTERN;
//    	return destFolderPath;
//    }
//    
//    public static String getErrorFolder(final String moduleName, String kafkaTopic){
//    	final boolean errorFolderFlag = true;
//    	String baseErrorFolderPath = getBaseFolder(moduleName, errorFolderFlag, kafkaTopic);
//    	String errorFolderPath = String.format("%s/%s/%s", baseErrorFolderPath,  ERROR_TYPE_PATTERN, DATE_PARTITION_PATTERN);
//    	return errorFolderPath;
//    }
    
    protected String getRoleARN() {
    	String roleName = role_name.getValueAsString() /* (String) this.getNode().tryGetContext("role_name") */;
    	Objects.requireNonNull(roleName, "CDK needs parameter: rolename");
    	
    	// return String.format("arn:aws:iam::%s:role/%s", getAWSAccountId(parent), roleName);
    	return String.format("arn:aws:iam::%s:role/%s", getCatalogId(), roleName);
    }

    public final String getFirehoseCdkId(String kafkaTopicname)  {
    	final String _MODULE_NAME = hyphenate(getModuleName());
    	final String cdkEnvName = getCdkEnvName();
    	final String shortTopicName = shortenTopicName(kafkaTopicname);
		final String hyphenatedTopicName = hyphenate(kafkaTopicname);
		
    	String firehoseCdkId = String.format(KDF_CDK_ID, getCdkQualifier(), _MODULE_NAME, shortTopicName, cdkEnvName);
//		if (firehoseCdkId.length() > FIREHOSE_NAME_ID_MAX_LENGTH) {
//			firehoseCdkId = String.format(KDF_CDK_ID, getCdkQualifier(), _MODULE_NAME, hyphenatedTopicName.hashCode(), cdkEnvName);
//		}
		return firehoseCdkId;
    }
    
	public final String getFirehoseName(String kafkaTopicname) {
		final String cdkEnvName = getCdkEnvName();
    	final String shortTopicName = shortenTopicName(kafkaTopicname);
		String firehoseName = String.format(KDF_NAME, getCdkQualifier(), shortTopicName, cdkEnvName);
		
		if (firehoseName.length() > FIREHOSE_NAME_ID_MAX_LENGTH) {
			firehoseName = String.format(KDF_NAME, getCdkQualifier(), shortTopicName.hashCode(), cdkEnvName);
		}
		return firehoseName;
	}
	
	private final static String shortenTopicName(String kafkaTopicName) {
		return kafkaTopicName.toLowerCase().startsWith("conformance_target_") ? kafkaTopicName.replace("conformance_target_", "") : kafkaTopicName.toLowerCase();
	}
    
    protected String getCatalogId() {
    	String awsAccountId =  this.getAccount();
    	Objects.requireNonNull(awsAccountId, "Fatal Error: Cannot find AWS Account ID");
    	
    	return awsAccountId;
    }
    
//    protected boolean isExistingGlueDb() {
//    	String existingGlueDbAsStr = existing_glue_db.getValueAsString();
//    	return Boolean.valueOf(existingGlueDbAsStr);
//    }
    
//    protected String getDatabaseId() {
//    	String glueDbName = glue_database.getValueAsString() /* (String) this.getNode().tryGetContext("glue_database") */;
//    	Objects.requireNonNull(glueDbName, "CDK needs parameter: gluedatabase");
//    	
//    	return glueDbName;
//    }
    
//    protected String getGlueTableName() {
//    	return getKafkaTopic();
//    }
    
    
    /**
     * Extract glue schema filename from S3 path
     * 
     * @param schemaFileS3Path	S3 path
     * @return Glue file name
     */
    public static String extractGlueSchemaFile(final String schemaFileS3Path) {
    	String[] parts = schemaFileS3Path.split("/");
    	String glueSchemaFileName = parts[parts.length -  1];
    	
    	return glueSchemaFileName;
    }
    
    /**
     * Extract kafka topic name from glue schema filename
     * 
     * @param glueSchemaFileName	filename format <topic_name>.glue.csv
     * @return Kafka topic name
     */
    public static String extractKafkaTopic(final String glueSchemaFileName) {
    	assert (null != glueSchemaFileName);
    	String[] parts = glueSchemaFileName.trim().split("\\.");
    	String topicName = parts[0];
    	
    	return topicName;
    }
    
    public static String hyphenate(final String _name) {
    	return _name.replace("_", "-");
    }
    
    public static String underscore(final String hyphenatedName) {
    	return hyphenatedName.replace("-", "_");
    }
    
    /**
     * 
     * @param awsRegion
     * @param linient		flag to use default region (us-east-1) if invalid region supplied
     * @return S3 client
     */
    public static S3Client getS3Client(String awsRegion, boolean linient) {
    	Region targetRegion = Region.of(awsRegion);
    	try {
    		assert (Region.regions().contains(targetRegion));
    		return getS3Client(targetRegion);
    	} catch (AssertionError e) {
    		if (linient) {
    			return getS3Client(DEFAULT_REGION);
    		} else {
    			System.err.println("ERROR: STRICK check - received region: "+ awsRegion);
    			throw e;
    		}
    	}
    }
    
    public static S3Client getS3Client(Region region) {
    	S3Client s3Cli = S3Client.builder()
    			.region(region)
    			.build();
    	assert (null != s3Cli);
    	return s3Cli;
    }
}
