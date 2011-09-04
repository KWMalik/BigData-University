package com.xebia.university.bigdata.twitter.mapreduce;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.map.ObjectMapper;

public class TwitterTermFrequencyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	public static final String XEBIA_TWITTER_TERMS = "xebia.twitter.terms";
	
	private Set<String> terms;
	
	private Text outputKey;
	private LongWritable outputValue;
	
	private DateFormat readFormat = new SimpleDateFormat("EEE MMM d H:m:s Z y"); //e.g. Fri Feb 25 14:04:46 +0000 2011
	private DateFormat writeFormat = new SimpleDateFormat("yyyyMMdd"); //e.g. 20110225
	
	private String textSeperator;
	
	protected void setup(Context context) throws IOException, InterruptedException {
		String[] termConfig = context.getConfiguration().get(XEBIA_TWITTER_TERMS).split("[\\s,]+");
		terms = new HashSet<String>(Arrays.asList(termConfig));
		
		outputKey = new Text();
		outputValue = new LongWritable();
		outputValue.set(1L);
		
		textSeperator = context.getConfiguration().get("mapred.textoutputformat.separator", "\t");
	}
	
	@SuppressWarnings("unchecked")
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String text = (String) ((Map<String, Object>) new ObjectMapper().readValue(value.toString(), Map.class)).get("text");
			
			String dateString = (String) ((Map<String, Object>) new ObjectMapper().readValue(value.toString(), Map.class)).get("created_at");
			Date date = readFormat.parse(dateString);
			String outputDateString = writeFormat.format(date);
			
			if (text != null) {
				String[] words = text.split("[\\s\\.\\,]+");
				for (String word : words) {
					if (terms.contains(word)) {
						outputKey.set(outputDateString + textSeperator + word);
						context.write(outputKey, outputValue);
					}
				}
			}
		} catch(Exception e) {
			context.getCounter(Counters.PARSE_ERRORS).increment(1L);
		}
	}
	
	public static enum Counters {
		PARSE_ERRORS;
	}
}
