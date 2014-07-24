package com.bhuvan.examples;

import java.io.IOException;

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

import com.custom.datatyps.EmployeeWritable;

public class EmployeeDetails {

	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		String userargs[] = new GenericOptionsParser(args).getRemainingArgs();
		Job myjob = new Job(conf,"bhuvan");
		
		myjob.setJarByClass(EmployeeDetails.class);
		myjob.setMapperClass(EmplyoyeeMapper.class);
		myjob.setReducerClass(EmployeeReducer.class);
		
		
		myjob.setMapOutputKeyClass(EmployeeWritable.class);
		myjob.setMapOutputValueClass(IntWritable.class);
		
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(IntWritable.class);
		
		myjob.setInputFormatClass(TextInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(myjob, new Path(userargs[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(userargs[1]));
		
		System.exit(myjob.waitForCompletion(true)? 0 : 1);
	}

public static class EmplyoyeeMapper extends Mapper<LongWritable, Text, EmployeeWritable, IntWritable>{
	EmployeeWritable emitkey  = new EmployeeWritable();
	IntWritable emitvalue = new IntWritable();
	
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
		
	String line = value.toString();
	String columns[] = line.split(",");
	System.out.println("columns.length os " + columns.length);
	if (columns.length == 5){
	
	emitkey.setfname(columns[0].toString());
	emitkey.setlname(columns[1].toString());
	emitkey.setage(Integer.parseInt(columns[2]));
	emitkey.setaddress(columns[3].toString());
	emitvalue.set(Integer.parseInt(columns[4]));
	
	System.out.println("firstname"+ emitkey.getfname());
	}
	context.write(emitkey, emitvalue);
	}

}
public static class EmployeeReducer extends Reducer<EmployeeWritable, IntWritable, Text, IntWritable>{
	Text emitkey = new Text();
	IntWritable emitvalue = new IntWritable();
	
	public void reduce(EmployeeWritable key,Iterable<IntWritable> value,Context context) throws IOException,InterruptedException{
		int addval = 0;
		for(IntWritable val : value){
		addval = addval + val.get();	
			
		}
		String temp = key.getfname() + "   " + key.getlname() + "  " + key.getage() + "   " + key.getaddress();
		emitkey.set(temp);
		emitvalue.set(addval);
		context.write(emitkey, emitvalue);
	}
}
}