package fr.htc.mapred.af;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
	 * 
	 * @author dmouchene
	 *
	 */
	public static class AnagramMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text sortedWordKey = new Text();

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// implémeneter le methode map : identifier la clé et la valeur
			// clé : le mort ordonné
			// value : le mot
			// value est une ligne de notre fichier ==> un mot du dictionnaire

			// trier le mot et le mettre le resultat dans : sortedWordKey

			String word = value.toString().trim();

			char[] arr = word.toCharArray();

			Arrays.sort(arr);

			sortedWordKey = new Text(new String(arr));

			// ajouter une pair (Key, Value) au contexte
			// context.write(sortedWordKey, wordValue);
			context.write(sortedWordKey, value);

		}
	}

	/**
	 * Reducer
	 * 
	 * @author dmouchene
	 *
	 */
	public static class AnagramReducer extends Reducer<Text, Text, Text, Text> {


		protected void reduce(Text key, Iterable<Text> words, Context context) throws IOException, InterruptedException {
			StringBuilder wordsSB = new StringBuilder();
			
			
			for (Text word : words) {
				wordsSB.append(" | ");
				wordsSB.append(word);
			}
			context.write(key, new Text(wordsSB.toString()));
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

			Configuration conf = new Configuration();
			Job job;
			job = new Job(conf, "Anagram Finder");

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
