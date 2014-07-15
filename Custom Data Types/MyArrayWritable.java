package com.custom.datatyps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MyArrayWritable implements Writable {
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

}
