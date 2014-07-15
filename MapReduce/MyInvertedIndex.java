package com.bhuvan.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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

public class MyInvertedIndex {
	public static final String filepath = "/stopword/english.stop";
	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
		// TODO Auto-generated method 
		Configuration conf = new Configuration();
		String userargs[] = new GenericOptionsParser(args).getRemainingArgs();
		
		Job myjob = new Job(conf,"Bhuvan");
		
		myjob.setJarByClass(MyInvertedIndex.class);
		myjob.setMapperClass(InvertedMapper.class);
		myjob.setReducerClass(InvertedReducer.class);
		
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(LongWritable.class);
		
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(Text.class);
		
		myjob.setInputFormatClass(TextInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(myjob, new Path(userargs[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(userargs[1]));
		
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}

public static class InvertedMapper extends Mapper<LongWritable, Text, Text,LongWritable>{
	int page_couter = 1;
	int line_counter = 0;
	String word = "";
	Text emitkey = new Text();
	LongWritable emitvalue = new LongWritable();
	ArrayList<String> stwords = new ArrayList<String>();
	
	public void setup(Context context) throws IOException{
		loadarry(context);	
	}
	void loadarry(Context context) throws IOException{
		FSDataInputStream in = null;
		BufferedReader br = null;
		FileSystem fs = FileSystem.get(context.getConfiguration());
		Path path = new Path(filepath);
		in = fs.open(path);
		br = new BufferedReader(new InputStreamReader(in));
		String line = "";
		String temp = "";
		while((line = br.readLine()) != null){
			//String[] arr = line.split(" ");
			
		//for (int i = 0;i < arr.length;i++){
			temp = line.replaceAll("[^a-zA-Z]+","");
			stwords.add(temp.toLowerCase());
			}
		}
		
	
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
		line_counter ++;
		if ((line_counter % 50) == 0){
			page_couter ++;
		}
		String line = value.toString();
		StringTokenizer strtoken = new StringTokenizer(line);
		
		while (strtoken.hasMoreTokens()){
			word = strtoken.nextToken().replaceAll("[^a-zA-Z]+","");
			if (!stwords.contains(word.toLowerCase())){	
			emitkey.set(word);
			emitvalue.set(page_couter);
			context.write(emitkey, emitvalue);
			}
		}
	}
}
public static class InvertedReducer extends Reducer<Text, LongWritable, Text, Text>{
	Text emitvalue = new Text();
	public void reduce(Text key,Iterable<LongWritable> value,Context context) throws IOException,InterruptedException{
		String ind = "";
		ArrayList<String> temp = new ArrayList<String>();
		
		for (LongWritable val : value){	
			if (!temp.contains(val.toString())){
			ind = ind + " " + val.toString() ;
			temp.add(val.toString());
			}
			}
			
		emitvalue.set(ind);
		context.write(key, emitvalue);
	//	temp.clear();
	}
		
	}
}
