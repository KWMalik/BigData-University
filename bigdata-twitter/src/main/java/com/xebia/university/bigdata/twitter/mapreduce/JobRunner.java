package com.xebia.university.bigdata.twitter.mapreduce;

import static java.lang.System.err;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobRunner extends Configured implements Tool {
	public static void main(String[] args) {
		try {
			System.exit(ToolRunner.run(new JobRunner(), args));
		} catch (Exception e) {
			err.println(e.getMessage());
			e.printStackTrace(err);
			System.exit(2);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			printUsage();
			System.exit(2);
		}
		
		String output = args[args.length - 1];
		String input = StringUtils.join(Arrays.copyOfRange(args, 0, args.length - 1), ',');
		
		Job job = createJob(getConf(), input, output);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	private Job createJob(Configuration conf, String input, String output) throws IOException {
		Job job = new Job(conf);
		
		job.setJarByClass(JobRunner.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		FileInputFormat.setInputPaths(job, input);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(output));
		
		job.setMapperClass(TwitterTermFrequencyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(TwitterTermFrequencyReducer.class);
		job.setCombinerClass(TwitterTermFrequencyReducer.class);
		
		return job;
	}

	private void printUsage() {
		err.println("Missing arguments!");
		err.println();
		err.println("Usage: hadoop jar <job-jar-name> input [input2 inputN] output");
		err.println();
		ToolRunner.printGenericCommandUsage(err);
	}
}
