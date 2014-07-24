package com.bhuvan.examples;

import java.io.IOException;
import java.util.StringTokenizer;

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

import com.custom.datatyps.MyArrayWritable;

public class MyArraySample {

	public static void main(String[] args)throws IOException,InterruptedException,ClassNotFoundException {
		Configuration conf = new Configuration();
		Job myjob = new Job(conf,"bhuvan");
		
		String userargs[] = new GenericOptionsParser(args).getRemainingArgs();
		
		myjob.setJarByClass(MyArraySample.class);
		myjob.setMapperClass(MyArrayMapper.class);
		myjob.setReducerClass(MyArrayReducer.class);
		
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(MyArrayWritable.class);
		
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(Text.class);
		
		myjob.setInputFormatClass(TextInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(myjob, new Path(userargs[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(userargs[1]));
		
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
		
		
	}
	public static class MyArrayMapper extends Mapper<LongWritable, Text, Text, MyArrayWritable>{
		Text emitkey = new Text();
		MyArrayWritable emitvalue = new MyArrayWritable();
		
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String[] columns = new String[27];
			String line = "";
			int[] maxmin = new int[2];
			int i = 0;
			int max = 0;
			int min = 9999;
			
			
			line =value.toString();
			StringTokenizer strtoken = new StringTokenizer(line);
			
			System.out.println(line);
			if (strtoken.hasMoreTokens()){
				System.out.println("has more elements");
				
			}
			else
			{
				System.out.println("no elements");
			}
			while(strtoken.hasMoreTokens()){
				
				columns[i] = strtoken.nextElement().toString();
			
				if ( i >  27 ) { break;}			
				System.out.println("i is " + i);
				if ( i >= 3){
					
					columns[i] = columns[i].replace("S", "").replace("C",  "").replace("P",  "");
					System.out.println("inside i > 3");
					max = Math.max(max, Integer.parseInt(columns[i]));
					min = Math.min(min, Integer.parseInt(columns[i]));
					
				}
				i ++;
			}
			maxmin[0] = min;maxmin[1] = max;
			
			emitkey.set(columns[0].toString() + columns[1].toString() + columns[2].toString());
			emitvalue.setintarray(maxmin);
			context.write(emitkey, emitvalue);
			i = 0;
			//System.out.println("arrat value" + emitvalue.getintarray()[0]);
		}

	}
public static class MyArrayReducer extends Reducer<Text, MyArrayWritable, Text, Text>{
	Text emitkey = new Text();
	Text emitvalue = new Text();
	
	public void reduce(Text key,Iterable<MyArrayWritable> value,Context context) throws IOException, InterruptedException{
		int[] maxminval = new int[2];
		for ( MyArrayWritable val : value){
			maxminval[0] = val.getintarray()[0];
			maxminval[1] = val.getintarray()[1];
		}
	
		emitkey.set(key);
		emitvalue.set(maxminval[0] + " " + maxminval[1]);
	
		context.write(emitkey, emitvalue);
		
		
	}
}

}
