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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class NewWordCount {

	public static void main(String[] args)throws IOException,InterruptedException,ClassNotFoundException{
		Configuration conf = new Configuration();
		Job myjob  = new Job(conf,"WordOcuurance");
		
		String userargs[] = new GenericOptionsParser(args).getRemainingArgs();
		
		myjob.setJarByClass(NewWordCount.class);
		myjob.setMapperClass(WordCountMapper.class);
		myjob.setReducerClass(WordCountReducer.class);
		
		
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(IntWritable.class);
		
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(Text.class);
		
		
		myjob.setInputFormatClass(TextInputFormat.class);
		myjob.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.addInputPath(myjob, new Path(userargs[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(userargs[1]));
		
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
		
		
		

	}

public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text emitkey = new Text();
	IntWritable emitvalue = new IntWritable();
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
		String line = value.toString();
		StringTokenizer strtoken = new StringTokenizer(line);
		while(strtoken.hasMoreTokens()){
			
			emitkey.set(strtoken.nextToken().replaceAll("[^a-zA-Z]+",""));
			emitvalue.set(1);
			context.write(emitkey, emitvalue);
		}
		
	}
}


public static class WordCountReducer extends Reducer<Text, IntWritable, Text, Text>{
	Text emitvalue = new Text();
	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
		String temp = "";
		for (IntWritable val : values){
			temp = temp + " " + val.toString();
		}
		emitvalue.set(temp);
		context.write(key, emitvalue);  
	}
}
}
