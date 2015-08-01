package com.sample.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class NamedClassExample {

	public static void main(String[] args) {
		String logfile = "hdfs://rlab:9000/logs/hadoop-bhuvan-namenode-rlab.log";
		SparkConf conf = new SparkConf();
		conf.setAppName("BhuvanTestAPP");
		JavaSparkContext sc = new JavaSparkContext("spark://rlab:7077", "appname");
	JavaRDD<String> logdata  = 	sc.textFile(logfile);
	JavaRDD<String>erros = logdata.filter(new Contains());
	long numas = erros.count();
	
	System.out.println("------------------------------");
	System.out.println("Count is" + numas);
	System.out.println("------------------------------");

	sc.close();
	}

}
class Contains implements Function<String, Boolean>{
	
/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

public Boolean call(String arg0) throws Exception {
	return arg0.contains("e");
}
}