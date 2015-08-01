package com.sample.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

public class FlatMapExample {

	public static void main(String[] args) {
	SparkConf conf = new SparkConf();
	conf.setAppName("Flatmapexample");
	conf.setMaster("spark://rlab:7077");
	JavaSparkContext sc = new JavaSparkContext(conf);
	
	JavaRDD<String> stringrdd = sc.parallelize(Arrays.asList("Bhuvan,Brijesh,nanjesh,Dev"));
	JavaRDD<String> words = stringrdd.flatMap(new FlatMapex());
	
	System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!!");
	System.out.println("First Element is" + words.first());
	System.out.println("!!!!!!!!!!!!!!!!!!!!!!");
	
	sc.close();
	}

}
class FlatMapex implements FlatMapFunction<String, String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Iterable<String> call(String arg0) throws Exception {
		return Arrays.asList(arg0.split("\\,"));
	}

	
	
}