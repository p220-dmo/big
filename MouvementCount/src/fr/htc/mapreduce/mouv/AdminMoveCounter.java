/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package fr.htc.mapreduce.mouv;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import au.com.bytecode.opencsv.CSVParser;
import fr.htc.mapreduce.mouv.AdminMoveCounter.TokenizerMapper;

public class AdminMoveCounter {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

		// initialize CSVParser as comma separated values
		private CSVParser csvParser = new CSVParser(';');
		private final static IntWritable one = new IntWritable(1);

		private static final String CSV_SEPERATOR = ";";
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			// Parse the input line
			String[] line = this.csvParser.parseLine(value.toString());
			String keyWord = line[2];
			word.set(keyWord);
			context.write(word, one);
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception {

		// Configuration du job
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Admin mouv count");

		// Definition de la classe java
		job.setJarByClass(AdminMoveCounter.class);

		// definition du mapper
		job.setMapperClass(TokenizerMapper.class);

		// definition du Combiner & Reducer
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		// definition des classes java pour les output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// definition des chemins des fichiers hdfs en entrée et en sortie

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// lancement du JOB
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
