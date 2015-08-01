package com.sample.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class DistinctExample {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Distinct Example");
		conf.setMaster("spark://rlab:7077");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<Integer> intrdd = sc.parallelize(Arrays.asList(1,1,2,3,3,5,6,7,4,3,4));
		JavaRDD<Integer> dist = intrdd.distinct();
		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!");
		System.out.println("distinct elemets are:" + dist.collect());
		System.out.println("!!!!!!!!!!!!!!!!!!!!!!!");
		sc.close();
	}
}
