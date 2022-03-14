package step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import main.Config;

public class Main {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "step1");
		job.setJarByClass(Main.class);

		// set mapper and reducer
		job.setMapperClass(CountMapper.class);
		job.setMapOutputKeyClass(BigramKey.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setPartitionerClass(PartitionerClass.class);

		job.setReducerClass(CountReducer.class);

		// set input
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(Config.THREE_GRAM_URL));

		// set combiner if local aggregation is on
		if (args[1].equals("true")) {
			job.setCombinerClass(CombinerClass.class);
		}

		// set output
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(Config.STEP1_OUTPUT));

		// wait for completion and exit
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
