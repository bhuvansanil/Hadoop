package com.bhuvan.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class WordCount2 {

	/**
	 * @param args
	 */
	
	public static void main(String[] args)throws IOException,InterruptedException,ClassNotFoundException {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		
		String userargs[] = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		Job myjob = new Job(conf,"bhuvan");
		
		//set class info
		myjob.setJarByClass(WordCount2.class);
		myjob.setMapperClass(WordCountMapper.class);
		myjob.setReducerClass(WordCountReducer.class);
		
		//set data info
		
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(IntWritable.class);
		
		//set outputformats
		
		myjob.setInputFormatClass(TextInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);
		
		// set path
		FileInputFormat.addInputPath(myjob, new Path(userargs[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(userargs[1]));
		
		//job execution
		
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
		
		
		
		
	
	}

	public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		IntWritable emitvalue = new IntWritable(1);
		Text word = new Text();
		String char_value = "";
		//int count = 0;
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String char_set = value.toString();
			StringTokenizer strtoken = new StringTokenizer(char_set);
			while (strtoken.hasMoreTokens()){
				word.set(strtoken.nextToken());
				context.write(word, emitvalue);
			}
			
			
		}
	}
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		public void reduce(Text key,Iterable<IntWritable> value ,Context context) throws IOException,InterruptedException{
			int sum = 0;
			for (IntWritable val : value){
				sum = sum + val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
}
