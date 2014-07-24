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


public class StockVolume2 {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
		
		Configuration conf = new Configuration();
		
		//parse generic options
		
		String userargs[] = new GenericOptionsParser(conf,args).getRemainingArgs();
		Job myjob = new Job(conf,"bhuvan");
		
		//set class info 
		
		myjob.setMapperClass(MyMapper2.class);
		myjob.setReducerClass(MyReducer2.class);
		myjob.setJarByClass(StockVolume2.class);
		
		//set data type info
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		
		//set input and output file
		
		myjob.setInputFormatClass(TextInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);
		
		//set input and output file locations
		
		FileInputFormat.addInputPath(myjob, new Path(userargs[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(userargs[1]));
		
		
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}

	public static class MyMapper2 extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		Text emitkey = new Text();
		LongWritable emitvalue = new LongWritable();
		String stock = "";
		long volume = 0L;
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String record = value.toString();
			String column[] = record.split("//t");
			if (column.length == 9){
				stock = column[1];
				volume = Long.valueOf(column[7]);
				emitkey.set(stock);
				emitvalue.set(volume);
				context.write(emitkey,emitvalue);
				
			}
		}
		
	}
	public static class MyReducer2 extends Reducer<Text, LongWritable, Text, LongWritable>{
		LongWritable emitvalue = new LongWritable();
		public void reduce(Text key,Iterable<LongWritable> value,Context context) throws IOException,InterruptedException{
			long sum = 0;
			for (LongWritable values : value){
				sum = sum + values.get();
				
			}
		emitvalue.set(sum);
		context.write(key,emitvalue);
		
		}
	}

}
