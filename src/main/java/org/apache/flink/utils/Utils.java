package org.apache.flink.utils;

import java.util.ArrayList;
import java.util.Random;

public class Utils {

	public ArrayList<Double> generateTimeSeries() {
		ArrayList<Double> r, e;
		double a = 0.01;
		
		r = this.numbersGenerator(3000);
		e = this.numbersGenerator(3000);
		
		int T = r.size();
		ArrayList<Double> Y = new ArrayList<Double>(); // time-serie
			
		// Fill up time-serie[0-2] with zeros.
		Y.add(0.0);
		Y.add(0.0);
		Y.add(0.0);
		
		for(int i=2; i<T; i++){
			double val = a + 0.1*r.get(i) + Y.get(i) + (1/T)*i*e.get(i);			
			Y.add(i+1, val);
		}
		
		return Y;
	}
	
	public ArrayList<Double> numbersGenerator(Integer length) {		
		Random random = new Random(10);
		int count = 0;			
		ArrayList<Double> list = new ArrayList();
		
		while(count < length) {
			list.add(random.nextGaussian());
			count += 1;
		}
		
		return list;		
	}		
	
}
