package com.sample.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SampleProg {

	public static void main(String[] args){
		String logfile = "hdfs://rlab:9000/logs/hadoop-bhuvan-namenode-rlab.log";
		SparkConf conf = new SparkConf();
		//conf.setAppName("TestAPP");
		String SPARK_HOME="/home/bhuvan/bigdata/spark-1.1.0";
		
		//JavaSparkContext sc = new JavaSparkContext(conf);
		JavaSparkContext sc = new JavaSparkContext("spark://rlab:7077", "appname");
		//JavaSparkContext sc = new JavaSparkContext("local","simpleapp", SPARK_HOME, new String[]{"target/spark-0.0.1-SNAPSHOT.jar"});
		JavaRDD<String> logdata = sc.textFile(logfile);
		
		long numAs = logdata.filter(new Function<String, Boolean>() {		
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			/**
			 * 
			 */
		//	private static final long serialVersionUID = 1L;
		//	@Override
			public Boolean call(String s) throws Exception {
				// TODO Auto-generated method stub
				return s.contains("e");
			}
		}).count();
	System.out.println("--------------------------------");
	System.out.println("number of error lines = " + numAs);
	System.out.println("----------------------------------");
	}
}
