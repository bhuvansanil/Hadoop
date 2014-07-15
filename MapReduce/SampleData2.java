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

public class SampleData2 {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Job myjob = new Job(conf,"bhuvan");
		
		String userargs[] = new GenericOptionsParser(conf,args).getRemainingArgs();

		//set class
		
		myjob.setJarByClass(SampleData2.class);
		myjob.setMapperClass(SampleDataMapper2.class);
		myjob.setReducerClass(SampleDataReducer2.class);
		
		//set mapoutput key class and output key/value class
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(LongWritable.class);
		
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(LongWritable.class);
		
		
		//set input and output formats
		
		myjob.setInputFormatClass(TextInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);
		
		//set input and output path
		
		FileInputFormat.addInputPath(myjob, new Path(userargs[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(userargs[1]));
		
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
	}

	public static class SampleDataMapper2 extends Mapper<LongWritable, Text, Text, LongWritable> {
		Text emitkey = new Text();
		LongWritable emitvalue = new LongWritable();
		String[] columns = new String[27];
		String data = "";
		int i = 0;
		public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException,ArrayIndexOutOfBoundsException{
			int i = 0;
			long temperature = 0L;
			data = value.toString();
			StringTokenizer strtoken = new StringTokenizer(data);
			while ( strtoken.hasMoreTokens()){
				columns[i] = strtoken.nextToken();
				
				if ( i > 27 ) { break; }
				if ( i >= 3 ){
					columns[i] = columns[i].replace("S", "").replace("C",  "").replace("P",  "");
					i ++;
				}
			}
				//emitkey.set(column[0].toString() + column[1].toString() + column[2].toString());
			//System.out.println("while loop completed");
				//int max = Integer.parseInt(column[3]);

				int max = 0;
				for ( int k = 3;k < i; k++ ){
					if (Integer.parseInt(columns[k]) > max){
						max = Integer.parseInt(columns[k]);
					}
				}
				temperature = Long.valueOf(max);
				emitkey.set(columns[0].toString() + columns[1].toString() + columns[2].toString());
				emitvalue.set(temperature);
				context.write(emitkey, emitvalue);
				i = 0;
			}
		}
	public static class SampleDataReducer2 extends Reducer<Text, LongWritable, Text, LongWritable>{
		String maxkey = "";
		int maxtemp = 0;
		Text emitkey = new Text();
		LongWritable emitvalue = new LongWritable();
		public void  reduce(Text key,Iterable<IntWritable> value,Context context) throws IOException,InterruptedException{
				for (IntWritable v : value) {
						if (v.get() > maxtemp)
						{
							maxtemp = v.get();
							maxkey = key.toString();
						}
		
		}
				emitkey.set(maxkey);emitvalue.set(maxtemp);
				context.write(emitkey, emitvalue);
		}
}
}
