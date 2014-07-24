package com.pig.mr;

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

public class StockMaxVolume {

	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		String userargs[] = new GenericOptionsParser(args).getRemainingArgs();
		Job myjob = new Job(conf,"bhuvan");
		
		myjob.setJarByClass(StockMaxVolume.class);
		myjob.setMapperClass(StockMapper.class);
		myjob.setReducerClass(StockReducer.class);
		
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(Text.class);
		
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(Text.class);
		
		myjob.setInputFormatClass(TextInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(myjob, new Path(userargs[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(userargs[1]));
		
		System.exit(myjob.waitForCompletion(true)? 0 : 1);
		
		
	}

public static class StockMapper extends Mapper<LongWritable, Text, Text, Text>{
	Text emitkey = new Text();
	Text emitval = new Text();
	
	public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
		
		String columns[] = value.toString().split(",");
		if (columns.length == 8){
			String temp = columns[1] + "\t" + columns[2] + "\t" + columns[3] + "\t" + columns[6];
			emitkey.set(columns[0].replace("(", ""));
			emitval.set(temp);
		}
		context.write(emitkey, emitval);
		System.out.println(emitkey.toString() + "\t" + emitval.toString());
	}
}

public static class StockReducer extends Reducer<Text, Text, Text, Text>{
	//Text emitkey = new Text();
	Text emitvalue = new Text();
	
	public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException{
		int maxvol = 0;
		float open = 0;
		float close = 0;
		String dates = "";
		for (Text val : values) {
			String cols[] = val.toString().split("\\t");
			if (Integer.parseInt(cols[3].toString()) > maxvol){
				dates = cols[0].toString(); 
				open = Float.parseFloat(cols[1].toString());
				close = Float.parseFloat(cols[2].toString());
				maxvol = Integer.parseInt(cols[3].toString());
			}
			
			
		}
		String temp = dates + "\t" + open + "\t" + close + "\t" + maxvol;
		emitvalue.set(temp);
		context.write(key, emitvalue);
	}
}
}
