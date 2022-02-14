package com.prod8ctive.infra.common;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.stream.Stream;

import static com.prod8ctive.infra.common.CdkConstants.DEFAULT_REGION;

import java.io.IOException;
import java.nio.file.*;
import java.util.stream.Collectors;

import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;

/*
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
*/
import software.amazon.awssdk.utils.Pair;

/**
 * 
 * @author chawl001
 *
 * @param <P>
 */
public final class FolderTraverser<P> implements Iterable<Pair<String,String>> {
	
	/*
	private S3Client s3Cli;
	private String s3Bucket;
	private String s3FolderPath;

	public FolderTraverser(@NotNull String s3BucketName, @NotNull String s3FolderPath) {
		this(DEFAULT_REGION.id(), s3BucketName, s3FolderPath);
	}
	
	public FolderTraverser(@NotNull String awsRegion, @NotNull String s3BucketName, @NotNull String s3FolderPath) {
		this.s3Cli = AbstractCdkBuildingBlock.getS3Client(awsRegion, true);
		
		this.s3Bucket = s3BucketName;
		this.s3FolderPath = s3FolderPath;
	}
	*/
	
	private boolean isLocal = true;
	private String schemaS3BucketName;
	private String schemaS3FolderPath;
	
	private FolderTraverser(final boolean localFlag) {
		this.isLocal = localFlag;
	}
	
	public FolderTraverser() {
		this(true);
	}
	
	public FolderTraverser(@NotNull final String bucketName, @NotNull final String topicsFolderName) {
		this(false);
		
		this.schemaS3BucketName = bucketName;
		this.schemaS3FolderPath = topicsFolderName;
	}
	
	@Override
	public Iterator<Pair<String,String>> iterator() {
      
		if (isLocal) {
			Path resourceFolderPath = Paths.get("./src/main/resources/cdk_test/");
			System.out.println("Folder path==="+ resourceFolderPath.toAbsolutePath());
			
			try (Stream<Path> stream = Files.walk(resourceFolderPath, 1)) {
				List<Pair<String,String>> detailsList = stream
		          .filter(file -> !Files.isDirectory(file))
		          .map(Path::getFileName)
		          .map(Path::toString)
	//		          .forEach(filePath -> {
	//		        	  String fileName = AbstractCdkBuildingBlock.extractGlueSchemaFile(filePath);
	//		        	  String topicName = AbstractCdkBuildingBlock.extractKafkaTopic(fileName);
	//		        	  
	//		        	  Pair<String, String> p = Pair.of(topicName, fileName);
	//		        	  System.out.println("Pair="+ p);
	//		        	  detailsList.add(p);
	//		          });
		          .map(fp -> {
		        	  String fileName = AbstractCdkBuildingBlock.extractGlueSchemaFile(fp);
		        	  String topicName = AbstractCdkBuildingBlock.extractKafkaTopic(fileName);
		        	  return Pair.of(topicName, fileName);
		          })
		          .collect(Collectors.toList());
				
				System.err.println("[DEBUG-DEBUG] =================================== ");
				System.err.println("[DEBUG-DEBUG] The FolderTraverser see total schema files: "+ detailsList.size());
				for (Pair<String,String> p : detailsList) 
					System.err.println("[DEBUG-DEBUG] File name: "+ p.right());
				System.err.println("[DEBUG-DEBUG] =================================== ");
				
				return detailsList.listIterator();
		    } catch (IOException ioEx) {
				ioEx.printStackTrace();
				
				// bubble-up exception
				throw new RuntimeException(ioEx);
			}
		} else {
			/*
	    	ListObjectsV2Request listObjects = ListObjectsV2Request
	                .builder()
	                .bucket(this.s3Bucket)
	                .prefix(this.s3FolderPath)
	                .build();

	        ListObjectsV2Response res = s3Cli.listObjectsV2(listObjects);
	        List<S3Object> s3Objects = res.contents();
	        
	        final List<Pair<String,String>> detailsList = new ArrayList<>();
	        s3Objects.stream()
	        	.forEach((s3Obj) -> 
	        		{
	        			String filePath = s3Obj.key();
	        			String fileName = AbstractCdkBuildingBlock.extractGlueSchemaFile(filePath);
	            		String topicName = AbstractCdkBuildingBlock.extractKafkaTopic(fileName);
	            		detailsList.add(Pair.of(topicName, fileName));
	        		}
	        	);
	        return detailsList.listIterator();
	        */
			
			Objects.requireNonNull(this.schemaS3BucketName, "TODO: ensure valid schema S3 Bucket Name in cstor");
			Objects.requireNonNull(this.schemaS3FolderPath, "TODO: ensure valid topics schema folder in cstor");
			
			throw new NotImplementedException("TODO: implement s3 based schema folder traverser");
		}
	}
	
	@Override
	public Spliterator<Pair<String,String>> spliterator() {
        throw new UnsupportedOperationException("spliterator() not supported!");
    }

}
