package fr.htc.mapred.sale;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SaleKpiManager {

	/**
	 * Mapper
	 * @author dmouchene
	 *
	 */
	public static class SaleMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
		
		private static final String CSV_FILE_SEPARATOR = ";";
		private static final int STORE_ID_CSV_INDEX = 4;
		private static final int STORE_SALE_CSV_INDEX = 5;
		private static final int STORE_COST_CSV_INDEX = 6;
		private static final int UNIT_SALE_CSV_INDEX = 7;

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//Value represente une ligne CSV
			String[] colunms = value.toString().split(CSV_FILE_SEPARATOR);
			
			Text storeIdKey = new Text(colunms[STORE_ID_CSV_INDEX]);
			
			Float storeSale = Float.valueOf(colunms[STORE_SALE_CSV_INDEX]);
			Float unitSale = Float.valueOf(colunms[UNIT_SALE_CSV_INDEX]);
			Float storeCost = Float.valueOf(colunms[STORE_COST_CSV_INDEX]);
			FloatWritable caValue = new FloatWritable((storeSale - storeCost) * unitSale );
			
			//implémeneter le methode map : identifier la clé et la valeur
			// clé : store id
			//value = stores_sale * sales_unit
			
			
			//ajouter une pair (Key, Value) au contexte
			 context.write(storeIdKey, caValue);
		}
	}

	/**
	 * Reducer
	 * @author dmouchene
	 *
	 */
	public static class SaleReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
		
		protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
			Float sum = Float.valueOf(0);
			for (FloatWritable ca : values) {
				sum+= ca.get();
			}
			context.write(key, new FloatWritable(sum));
		}

	}
	
	
	
	
	
	

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

		Configuration conf = new Configuration();
		Job job = new Job(conf, "KPI Manager");

		job.setJarByClass(SaleKpiManager.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(SaleMapper.class);
		job.setCombinerClass(SaleReducer.class);
		job.setReducerClass(SaleReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}