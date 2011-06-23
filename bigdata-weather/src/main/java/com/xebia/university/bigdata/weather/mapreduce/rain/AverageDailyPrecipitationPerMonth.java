package com.xebia.university.bigdata.weather.mapreduce.rain;

import static java.lang.System.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AverageDailyPrecipitationPerMonth {
    public static Job createJob(Configuration configuration, Path inputPath, Path outputPath) throws IOException {
        // the default separator char is a tab and we want to produce CSV-compatible data
        configuration.set("mapred.textoutputformat.separator", ",");

        Job job = new Job(configuration);
        job.setJarByClass(AverageDailyPrecipitationPerMonth.class);
        job.setJobName(jobName());

        // one reducer will result in one CSV file and our output won't be big enough to cause problems.
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(PrecipitationPerMonthMapper.class);
        // no combiner as average isn't commutative
        job.setReducerClass(AverageDailyPrecipitationPerMonthReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        return job;
    }

    public static String jobName() {
        return "average-daily-precipitation-per-month";
    }

    public static void printJobDescription() {
        out.println(jobName() + ":");
        out.println("  Processes KNMI weatherstation measurement files into a CSV file containing the station identifier,");
        out.println("  the month (yyyyMM) and the the average daily precipitation (in 0.1 mm) per month");
        out.println("  Some sample output data:");
        out.println();
        out.println("  # station, #month, #precipitation");
        out.println("  210,200501,18");
    }
}
