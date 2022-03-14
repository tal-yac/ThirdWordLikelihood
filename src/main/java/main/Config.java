package main;

import com.amazonaws.services.ec2.model.InstanceType;

public class Config {

	public static final String EMR_RELEASE_LABEL = "emr-5.16.0";
	public static final String HADOOP_VERSION = "2.8.4";
	public static final String EC2_KEY_NAME = "vockey";
	public static final String REGION = "us-east-1";
	public static final String SUBNET_ID = "subnet-0df26357";
	public static final int NUM_OF_INSTANCES = 9;
	public static final String INSTANCE_TYPE = InstanceType.M4Large.toString();

	public static final String BUCKET_NAME = "talondspsassignment2";
	public static final String BUCKET_URL = "s3n://" + BUCKET_NAME + "/";
	public static final String LOG_DIR = BUCKET_URL + "logs/";

	public static final String ONE_GRAM_URL = "s3://datasets.elasticmapreduce/"
			+ "ngrams/books/20090715/heb-all/1gram/data";
	public static final String TWO_GRAM_URL = "s3://datasets.elasticmapreduce/"
			+ "ngrams/books/20090715/heb-all/2gram/data";
	public static final String THREE_GRAM_URL = "s3://datasets.elasticmapreduce/"
			+ "ngrams/books/20090715/heb-all/3gram/data";

	public static final String JAR_NAME = "jar/steps.jar";
	public static final String JAR_URL = BUCKET_URL + JAR_NAME;
	
	public static final String STEP1_OUTPUT = BUCKET_URL + "step1output/";
	public static final String STEP2_OUTPUT = BUCKET_URL + "step2output/";
	public static final String STEP3_OUTPUT = BUCKET_URL + "step3output/";
	public static final String STEP4_OUTPUT = BUCKET_URL + "step4output/";
	public static final String STEP5_OUTPUT = BUCKET_URL + "step5output/";


	public static final String WILDCARD = "*";
	public static final String SUM_ALL = "sum.all";

}
