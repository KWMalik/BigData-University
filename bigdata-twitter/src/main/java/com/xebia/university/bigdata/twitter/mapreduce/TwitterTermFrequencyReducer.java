package com.xebia.university.bigdata.twitter.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwitterTermFrequencyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
	private LongWritable outputValue;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		outputValue = new LongWritable();
	}
	
	protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
		long sum = 0;
		
		for (LongWritable value : values) {
			sum += value.get();
		}
		
		outputValue.set(sum);
		context.write(key, outputValue);
	}
}
