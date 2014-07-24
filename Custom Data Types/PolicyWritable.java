package com.custom.datatyps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class PolicyWritable implements WritableComparable<PolicyWritable>{
	String policy_number = "";
	String transaction = "";
	
	public void readFields(DataInput input) throws IOException{
		policy_number = input.readUTF();
		transaction = input.readUTF();
	}
	public void write(DataOutput output) throws IOException{
		output.writeUTF(policy_number);
		output.writeUTF(transaction);
	}

	public void setpnumbertrans(String pnumber,String trans){
		this.policy_number = pnumber;
		this.transaction = trans;
	}
	
	public String getpnumber(){
		return policy_number;
	}
	public String gettrans(){
		return transaction;
	}

	public int compareTo(PolicyWritable policy){
		return (policy_number.compareTo(policy.policy_number) != 0) ? policy_number.compareTo(policy.policy_number): transaction.compareTo(policy.transaction); 
	}

}
