package fr.htc.mapred.af;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AnagramFinder {

	/**
	 * Mapper
	 * @author dmouchene
	 *
	 */
	public static class AnagramMapper extends Mapper<LongWritable, Text, Text, Text> {

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		}
	}

	/**
	 * Reducer
	 * @author dmouchene
	 *
	 */
	public static class AnagramReducer extends Reducer<Text, Text, IntWritable, Text> {

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
		
		}

	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "Anagram Finder");

		job.setJarByClass(AnagramFinder.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(AnagramMapper.class);
		job.setCombinerClass(AnagramReducer.class);
		job.setReducerClass(AnagramReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}