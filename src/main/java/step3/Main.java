package step3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import main.Config;

public class Main {

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "step3");

		// set main class
		job.setJarByClass(Main.class);

		// set mapper and reducer
		job.setMapperClass(ThirdMapper.class);
		job.setMapOutputKeyClass(BigramKey.class);
		job.setMapOutputValueClass(NGramValue.class);

		job.setReducerClass(ThirdReducer.class);

		// set partitioner
		job.setPartitionerClass(PartitionerClass.class);

		// set input
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(Config.STEP2_OUTPUT));

		// set output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(Config.STEP3_OUTPUT));

		// wait for completion and exit
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
