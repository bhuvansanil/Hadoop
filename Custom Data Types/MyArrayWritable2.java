package com.custom.datatyps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MyArrayWritable2 implements WritableComparable<MyArrayWritable2> {
int[] intarray = new int[2];

public void readFields(DataInput datain) throws IOException{
	for (int i = 0 ;i < 2 ;i ++){
		intarray[i] = datain.readInt();	
		
	}
	}

public void write(DataOutput dataout) throws IOException{
	
	for (int i = 0; i < 2; i ++){
		dataout.writeInt(intarray[i]);
	}
}

public void setintarray(int[] inarray){
	
	for (int i = 0; i < 2; i++ ){
		this.intarray[i] = inarray[i];
	}
}

public int[] getintarray(){
		return intarray;
}

public int compareTo(MyArrayWritable2 arraywritable2){
//return (Integer) ((Integer.valueOf(intarray[0]).compareTo(Integer.valueOf(arraywritable2.intarray[0])) !=0 ) ? Integer.valueOf(intarray[0]).compareTo(Integer.valueOf(arraywritable2.intarray[0])) !=0 ) : Integer.valueOf(intarray[1]).compareTo(Integer.valueOf(arraywritable2.intarray[1]))); 
//Integer a1 = new Integer(intarray[0]);
//Integer a2 = new Integer(intarray[1]);

return ((Integer.valueOf(intarray[0]).compareTo(Integer.valueOf(intarray[0])))!=0 ? Integer.valueOf(intarray[0]).compareTo(Integer.valueOf(intarray[0])) : Integer.valueOf(intarray[1]).compareTo(Integer.valueOf(intarray[1])));	
	

}
}
