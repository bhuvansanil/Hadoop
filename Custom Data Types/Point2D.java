package com.custom.datatyps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class Point2D implements Writable{
public float x;
public float y;
 
public Point2D(float x,float y){
	this.x = x;
	this.y = y;
	
}
public Point2D(){
	this(0.0f, 0.0f);
}
public void write (DataOutput out) throws IOException{
	out.writeFloat(x);
	out.writeFloat(y);
	
}
public void readFields(DataInput in) throws IOException{
	x = in.readFloat();
	y = in.readFloat();
}
public String toString(){
	return Float.toString(x) + "," + Float.toString(y);
}
}
