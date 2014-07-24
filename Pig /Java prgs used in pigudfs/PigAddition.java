package com.bhuvan.pigudfs;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class PigAddition extends EvalFunc<Integer>{
	public Integer exec(Tuple input) throws IOException{
		if (input == null || input.size() == 0){
			return null;
		}
	Integer num1 = (Integer)input.get(0);
	Integer num2 = (Integer)input.get(1);
	Integer sum = num1 + num2;
	return sum;
	}

}
