package com.bhuvan.pigudfs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class MaxVolume2 extends EvalFunc<Tuple> {

	//@SuppressWarnings("null")
	@Override
	public Tuple exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0){
		return null;
	}
		DataBag datein = (DataBag)input.get(0);
		DataBag volin = (DataBag)input.get(1);
	
		
		Iterator<Tuple> dateiterator = datein.iterator();
		Iterator<Tuple> voliterator = volin.iterator();
		TupleFactory tf = TupleFactory.getInstance();
		Tuple outfields = tf.newTuple(2);
		
		Tuple tmax = null;
	
		Tuple tdate = null;
		
		Integer max = Integer.MIN_VALUE;
		//String maxvoldate = null;
		
		while (voliterator.hasNext()){
			tmax = voliterator.next();
			tdate = dateiterator.next();
			
			if ((Integer)tmax.get(0) > max){
				outfields.set(0, tmax);
				outfields.set(1,tdate);
			//	outfields.set(1, (String)dateiterator.next().get(0));
				
			}
			/*else
			{
			dateiterator.next();
			}*/
		}
		return outfields;
		
		
	}
	
	
}
