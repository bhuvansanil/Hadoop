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

public class StockVolume3 {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws ClassNotFoundException,IOException,InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		
		//args
		
		String userargs[] = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		Job myjob = new Job(conf,"bhuvan");
		
		//set class
		
		myjob.setMapperClass(MyMapper3.class);
		myjob.setReducerClass(MyReducer3.class);
		myjob.setJarByClass(StockVolume.class);
		
		// data type
		
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		
		//set formats
		
		myjob.setInputFormatClass(TextInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);
		
		//set input and file output locations
		
		FileInputFormat.addInputPath(myjob, new Path(userargs[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(userargs[1]));
		
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
		
	}
public static class MyMapper3 extends Mapper<LongWritable, Text, Text, LongWritable>
	{
		Text emitkey = new Text();
		LongWritable emitvalue = new LongWritable();
		String stock ="";
		long volume = 0L;
		
		
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException
		{
			String record = value.toString();
			String column[] = record.split("//t");
			if (column.length == 9){
				stock = column[10];
				volume = Long.valueOf(column[7]);
				emitkey.set(column[1]);
				emitvalue.set(volume);
				context.write(emitkey,emitvalue);
			
			}
		}

	}
public static class MyReducer3 extends Reducer<Text, LongWritable, Text, LongWritable>{
	
	LongWritable emitvalue = new LongWritable();
		long sum = 0L;
		public void map(Text key,Iterable<LongWritable> value,Context context) throws IOException,InterruptedException{
			for(LongWritable values : value){
				sum = sum + values.get();
				
			}
			emitvalue.set(sum);
			context.write(key,emitvalue);
		}
}

}
