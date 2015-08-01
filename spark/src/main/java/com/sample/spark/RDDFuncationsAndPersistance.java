package com.sample.spark;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

public class RDDFuncationsAndPersistance {

	public static void main(String[] args) {
		//String logfile = "hdfs://rlab:9000/logs/hadoop-bhuvan-namenode-rlab.log";
		SparkConf conf = new SparkConf();
		conf.setAppName("RDD Functions");
		conf.setMaster("spark://rlab:7077");
		JavaSparkContext sc  = new JavaSparkContext(conf);
		JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1,2,3,4,5,6,7,8,9,2,2,2));
		//new StorageLevel();
		rdd1.persist(StorageLevel.MEMORY_ONLY());
		System.out.println("Collect data: " + rdd1.collect());
		System.out.println("Collect data: " + rdd1.count());
		System.out.println("----RDD COUNT BY VALUE------");
		System.out.println(rdd1.countByValue());
		System.out.println("----RDD by Take------");
		System.out.println(rdd1.take(2));
		System.out.println("RDD TOP");
		System.out.println(rdd1.top(10));
		System.out.println("take ordered:" +rdd1.takeOrdered(2));
		System.out.println("Take Sample: " + rdd1.takeSample(false, 2));
		sc.close();
		
		
	}
	

}

