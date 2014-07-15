package com.bhuvan.examples;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

import com.custom.datatyps.PolicyWritable;

public class PolicyData {

	public static void main(String[] args)throws IOException,InterruptedException,ClassNotFoundException {
		Configuration conf = new Configuration();

		conf.setBoolean("mapreduce.task.profile", true);
		conf.set("mapreduce.task.profile.params", "-agentlib:hprof=cpu=samples,heap=sites,depth=6,force=n,thread=y,verbose=n,file=%s");
		conf.set("mapreduce.task.profile.maps", "0-2");
		conf.set("mapreduce.task.profile.reduces", "");

		Job myjob = new Job(conf,"bhuvan");

		String userargs[] = new GenericOptionsParser(args).getRemainingArgs();

		myjob.setJarByClass(PolicyData.class);
		myjob.setMapperClass(PolicyMapper.class);
		myjob.setCombinerClass(PolicyReducer.class);
		myjob.setReducerClass(PolicyReducer.class);
		//myjob.setPartitionerClass(PolicyPartitioner.class);


		myjob.setMapOutputKeyClass(PolicyWritable.class);
		myjob.setMapOutputValueClass(DoubleWritable.class);

		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(DoubleWritable.class);

		myjob.setInputFormatClass(TextInputFormat.class);
		myjob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(myjob, new Path(userargs[0]));
		FileOutputFormat.setOutputPath(myjob, new Path(userargs[1]));

		System.exit(myjob.waitForCompletion(true) ? 0 : 1);

	}

	public static class PolicyMapper extends Mapper<LongWritable, Text, PolicyWritable, DoubleWritable>{
		PolicyWritable emitkey = new PolicyWritable();
		DoubleWritable emitvalue = new DoubleWritable();


		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String columns[] = value.toString().split("\\t");
			double amt = 0.0;
			if (columns.length == 3){
				amt = Double.valueOf(columns[2]);
				emitkey.setpnumbertrans(columns[0], columns[1]);
				emitvalue.set(amt);
				context.write(emitkey, emitvalue);
			}			
		}	
	}

	public static class PolicyReducer extends Reducer<PolicyWritable, DoubleWritable, Text, DoubleWritable>{

		Text emitkey = new Text();
		DoubleWritable emitvalue = new DoubleWritable();
		Map<String, String> mapkeys = null;
		public void setup(Context context) throws IOException{
			loadkeys(context);
		}
		void loadkeys(Context context) throws IOException{
			mapkeys = new HashMap<String, String>();
			mapkeys.put("TK", "Taken Transaction");
			mapkeys.put("SU", "Surrendered");
			mapkeys.put("TB", "Monthly Transaction");
		}
		public void reduce(PolicyWritable key,Iterable<DoubleWritable> value,Context context) throws IOException,InterruptedException{
			double sum = 0.0;
			for (DoubleWritable val : value){
				sum = sum + val.get();

			}

			String temp = key.getpnumber() + "  " + mapkeys.get(key.gettrans());
			emitkey.set(temp);
			emitvalue.set(sum);
			context.write(emitkey, emitvalue);
		}
	}
	/*public static class PolicyPartitioner extends Partitioner<PolicyWritable, DoubleWritable>{
	public int getPartition(PolicyWritable key,DoubleWritable value,int numReduceTasks){
		String pnum = key.getpnumber();
		if ( numReduceTasks == 0){
			return 0;
		}

		if ( pnum == "9008163629"){
			return 1 % numReduceTasks;
		}
		else
			return 2 % numReduceTasks;
	}
}*/
}
