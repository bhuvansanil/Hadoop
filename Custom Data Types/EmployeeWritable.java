package com.custom.datatyps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class EmployeeWritable implements WritableComparable<EmployeeWritable>{
	String fname = "";
	String lname = "";
	int  age = 0;
	String address = "";
	
		public void readFields(DataInput input) throws IOException{
			fname = input.readUTF();
			lname = input.readUTF();
			age = input.readInt();
			address = input.readUTF();
		}
		public void write(DataOutput output) throws IOException{
			output.writeUTF(fname);
			output.writeUTF(lname);
			output.writeInt(age);
			output.writeUTF(address);
		
		}

		public void setfname(String firstname){
			this.fname = firstname;
		}
		public void setlname(String lastname){
			this.lname = lastname;
		}
		public void setage(int age){
			this.age = age;
		}
		public void setaddress(String add){
			this.address = add;
		}
		public String getfname(){
			return fname;
		}

		public String getlname(){
			return lname;
		}
		public int getage(){
			return age;
		}
		public String getaddress(){
			return address;
		}
		
		public int compareTo(EmployeeWritable employee){
			return (fname.compareTo(employee.fname)!= 0) ? fname.compareTo(employee.fname) : lname.compareTo(employee.lname);
		}
		
}
