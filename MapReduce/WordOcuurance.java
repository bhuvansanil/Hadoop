package com.bhuvan.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordOcuurance {

	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
		Configuration conf = new Configuration();
		Job myjob = new Job(conf,"Word Occurance");
		
		String userargs[] = new GenericOptionsParser(args).getRemainingArgs();
		
		myjob.setJarByClass(WordOcuurance.class);
		myjob.setMapperClass(WordOccurMapper.class);
		myjob.setReducerClass(WorOccurReducer.class);
		
		
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(IntWritable.class);
		
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(Text.class);
		
		myjob.setInputFormatClass(SequenceFileInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);

		
		FileInputFormat.addInputPath(myjob, new Path(userargs[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(userargs[1]));
		
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
		
		
		

	}

public static class WordOccurMapper extends Mapper<Text, Text, Text, IntWritable>{
	IntWritable emitvalue = new IntWritable();
	
	public void map(Text key,Text value,Context context)throws IOException,InterruptedException{
		String line = value.toString();
		int total = 0;
		StringTokenizer strtoken = new StringTokenizer(line);
		while(strtoken.hasMoreTokens()){
			total ++;
		}
		emitvalue.set(total);
		context.write(key, emitvalue);
	}
	
}
public static class WorOccurReducer extends Reducer<Text,IntWritable, Text, Text>{
	Text emitvalue = new Text();
	public void reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException, InterruptedException{
		String temp = "";
	for (IntWritable val : value){
		temp = val.toString();
		emitvalue.set(temp);
	}
	context.write(key, emitvalue);
	}
}
	
}
