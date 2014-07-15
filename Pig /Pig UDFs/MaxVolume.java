package com.bhuvan.pigudfs;


import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

public class MaxVolume extends EvalFunc<String>{

	@Override
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0){
		return null;
			}
		DataBag datein = (DataBag)input.get(0);
		DataBag vol = (DataBag)input.get(1);
		
		Iterator<Tuple> dateitr = datein.iterator();
		Iterator<Tuple> volitr = vol.iterator();
		Tuple maxvol = null;
		Tuple maxdate = null;
		
		Integer max = Integer.MIN_VALUE;
		String maxvoldate = null;
		while ( volitr.hasNext()){
			maxvol = volitr.next();
			maxdate = dateitr.next();
			if ((Integer)maxvol.get(0) > max ){
				max = (Integer)maxvol.get(0);	
				maxvoldate = (String)maxdate.get(0);
			}
		}
		
		String passval = max + "," + maxvoldate;
		return passval;
}
}
