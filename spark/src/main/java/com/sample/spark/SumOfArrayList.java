package com.sample.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SumOfArrayList {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("Sum of array integers" );
		JavaSparkContext sc = new JavaSparkContext("spark://rlab:7077", "bhuvan");
		
		
		JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7));
		JavaRDD<Integer> result = rdd.map(new sum());
		
		System.out.println("-----------------------------");
		System.out.println("Sum is " + result.collect());
		System.out.println("-----------------------------");
		sc.close();
	}


}

class sum implements Function<Integer, Integer>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public Integer call(Integer arg0) throws Exception {
		return arg0 + arg0;
	}
	
}