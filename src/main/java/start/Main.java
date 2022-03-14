package start;

import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

import main.Config;

public class Main {

	public static void main(String[] args) {

		AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder
				.defaultClient();
		System.out.println("Connected to EMR");

		// Configure first step, input 3-gram
		StepConfig stepConfig1 = new StepConfig().withName("step1")
				.withActionOnFailure("TERMINATE_JOB_FLOW")
				.withHadoopJarStep(new HadoopJarStepConfig().withJar(Config.JAR_URL)
						.withArgs("1", "true"));

		// Configure second step, input is the output of first step
		StepConfig stepConfig2 = new StepConfig().withName("step2")
				.withActionOnFailure("TERMINATE_JOB_FLOW").withHadoopJarStep(
						new HadoopJarStepConfig().withJar(Config.JAR_URL).withArgs("2"));

		StepConfig stepConfig3 = new StepConfig().withName("step3")
				.withActionOnFailure("TERMINATE_JOB_FLOW").withHadoopJarStep(
						new HadoopJarStepConfig().withJar(Config.JAR_URL).withArgs("3"));

		StepConfig stepConfig4 = new StepConfig().withName("step4")
				.withActionOnFailure("TERMINATE_JOB_FLOW").withHadoopJarStep(
						new HadoopJarStepConfig().withJar(Config.JAR_URL).withArgs("4"));

		System.out.println("Configured all steps");

		JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
				.withInstanceCount(Config.NUM_OF_INSTANCES)
				.withMasterInstanceType(Config.INSTANCE_TYPE)
				.withSlaveInstanceType(Config.INSTANCE_TYPE)
				.withHadoopVersion(Config.HADOOP_VERSION)
				.withEc2KeyName(Config.EC2_KEY_NAME)
				.withKeepJobFlowAliveWhenNoSteps(false)
				.withPlacement(new PlacementType("us-east-1a"));

		// Create a flow request including all the steps
		RunJobFlowRequest runFlowRequest = new RunJobFlowRequest().withName("start")
				.withInstances(instances)
				.withSteps(/* stepConfigDebug, */stepConfig1, stepConfig2, stepConfig3,
						stepConfig4) // TODO:
				// add
				// steps

				.withLogUri(Config.LOG_DIR).withServiceRole("EMR_DefaultRole")
				.withJobFlowRole("EMR_EC2_DefaultRole")
				.withReleaseLabel(Config.EMR_RELEASE_LABEL);

		System.out.println("Created job flow request");

		// Run the flow
		RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
		String jobFlowId = runJobFlowResult.getJobFlowId();
		System.out.println("Ran job flow with id: " + jobFlowId);
	}
}
