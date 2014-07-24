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


public class SampleOut3 {

	/*
	 * @param args
	 */
	public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		String userargs[] = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		//create job
		
		Job myjob = new Job(conf,"bhuvans job");
		
		//set classes
		
		myjob.setJarByClass(SampleOut3.class);
		myjob.setMapperClass(SampleDataMapper3.class);
		myjob.setReducerClass(SampleReducer3.class);
		
		//set input/output data formats
		
		myjob.setMapOutputKeyClass(Text.class);
		myjob.setMapOutputValueClass(LongWritable.class);
		
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(Text.class);
		
		//set input and output formats
		
		myjob.setInputFormatClass(TextInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);
		
		//set input and output file locations
		
		FileInputFormat.addInputPath(myjob, new Path(userargs[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(userargs[1]));
		
		System.exit(myjob.waitForCompletion(true) ? 0 : 1);
		
		
		
		
		

	}

public static class SampleDataMapper3 extends Mapper<LongWritable, Text, Text, LongWritable>{
	Text emitkey = new Text();
	LongWritable emitvalue = new LongWritable();
	String data = "";
	String[] columns = new String[27];
	//ArrayList columns = new ArrayList();
	int i = 0;
	public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException,ArrayIndexOutOfBoundsException{
		data = value.toString();
		long temperature = 0L;
		int i = 0;
		StringTokenizer stripstring = new StringTokenizer(data);
		while (stripstring.hasMoreTokens()){
		 columns[i] = stripstring.nextToken();
			if (i > 27){break;}	
		 	if ( i >= 3){
					columns[i] = columns[i].replace("C", "").replace("S", "").replace("P", "");
					}
			i ++;
				}
		
//		String record = value.toString();
//		String columns[] = record.split("\\t");
//		i = columns.length;
		//int max = Integer.parseInt(columns[3]);
		int max = 0;
		for (int k = 3; k < i ;k++){
			if ( Integer.parseInt(columns[k]) > max ) {
				max = Integer.parseInt(columns[k]); 
			}
			
		}
	temperature = Long.valueOf(max);	
	emitkey.set(columns[0].toString() + columns[1].toString() + columns[2].toString());
	emitvalue.set(temperature);
	context.write(emitkey, emitvalue);
	i =0;
	}
}
public static class SampleReducer3 extends Reducer<Text, LongWritable, Text, LongWritable>
{
	LongWritable emitvalue = new LongWritable();
	String maxkey = "";
	Text emitkey = new Text();
	public void reduce(Text key,Iterable<LongWritable> value,Context context) throws IOException,InterruptedException{
//		nt i = 0;
		long max = 0L;
		for (LongWritable v : value){
			if ( v.get() > max ){
				max = v.get();
				maxkey = key.toString();
		
			}
		}
		emitvalue.set(max);
		emitkey.set(maxkey);
		
		context.write(emitkey, emitvalue);
	}
}
}

