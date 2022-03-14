package main;

import java.io.File;
import java.util.Iterator;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Storage {

	private static final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();

	private static boolean isBucketExist(String bucketName) {
		for (Bucket bucket : s3.listBuckets()) {
			if (bucket.getName().equals(bucketName)) {
				System.out.println(
						"bucket found, no need to create new bucket: " + bucketName);
				return true;
			}
		}
		System.out.println("bucket not found, creating new bucket: " + bucketName);
		return false;
	}

	public static void deleteBucket(String bucketName) {
		try {
			ObjectListing objectListing = s3.listObjects(bucketName);
			while (true) {
				Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries()
						.iterator();
				while (objIter.hasNext()) {
					s3.deleteObject(bucketName, objIter.next().getKey());
				}
				if (objectListing.isTruncated()) {
					objectListing = s3.listNextBatchOfObjects(objectListing);
				}
				else {
					break;
				}
			}
			s3.deleteBucket(bucketName);
		} catch (AmazonServiceException e) {
			e.printStackTrace();
		} catch (SdkClientException e) {
			e.printStackTrace();
		}
	}

	public static void createBucket(String bucketName) {
		try {
			s3.createBucket(bucketName);
		} catch (AmazonClientException e) {
			e.printStackTrace();
		}
	}

	public static void uploadJarToBucket() {
		try {
			File f = new File(Config.JAR_NAME);
			PutObjectRequest req = new PutObjectRequest(Config.BUCKET_NAME,
					Config.JAR_NAME, f).withCannedAcl(CannedAccessControlList.PublicRead);
			s3.putObject(req);
		} catch (AmazonClientException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		if (isBucketExist(Config.BUCKET_NAME))
			deleteBucket(Config.BUCKET_NAME);
		else {
			createBucket(Config.BUCKET_NAME);
			uploadJarToBucket();
		}
	}

}
