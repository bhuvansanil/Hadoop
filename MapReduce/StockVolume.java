package com.bhuvan.examples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class StockVolume {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException	 {
		//create conf object
		Configuration conf = new Configuration();
		//parse generic options
		
		String userargs[] = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		Job myjob = new Job(conf,"bhuvan");
		//set class info
		myjob.setJarByClass(StockVolume.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
			
		
		//set data type info 
		
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(LongWritable.class);
		
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		
		//set output formats
		myjob.setInputFormatClass(TextInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);
		
		//set input and output file locations
		
		FileInputFormat.addInputPath(myjob, new Path(userargs[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(userargs[1]));
		
		//job submission
		
		System.exit(myjob.waitForCompletion(true) ? 0: 1);
		
		

	}
//develop map class
	
	public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		Text emitkey = new Text();
		LongWritable emitvalue = new LongWritable();
		String stock = "";
		long volume = 0L;
		public void map(LongWritable key, Text value, Context context ) throws IOException, InterruptedException {
			String record = value.toString();
			String columns[] = record.split("\\t");
			if (columns.length == 9){
				stock = columns[1];
				volume = Long.valueOf(columns[7]);
				emitkey.set(stock);
				emitvalue.set(volume);
				context.write(emitkey, emitvalue);
				
			}
		}
	}
	//Develop reducer class
	public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		LongWritable emitvalue = new LongWritable();
		public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (LongWritable value : values) {
				sum = sum + value.get();
				
			}
			emitvalue.set(sum);
			context.write(key, emitvalue);
		}
	}
}
