package org.apache.flink;

import java.util.ArrayList;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public final class SAXCalculi {
	
	public static ArrayList<Double> timeSerie;
	private static double ds_summation; // squares of time serie items
	private static double ds_sum;
	private static double ds_count;
	private static double ds_avg;
	private static double ds_stDev;
	public static double norm_threshold;
	
	public SAXCalculi() {
		ds_summation = 0D;
		SAXCalculi.ds_sum = 0D;
		SAXCalculi.ds_count = 0D;
		SAXCalculi.ds_avg = 0D;
		SAXCalculi.ds_stDev = 0D;
		SAXCalculi.norm_threshold = 2.0;
		SAXCalculi.timeSerie = new ArrayList<Double>();
	}
	
	public static void setSummation(Double value) {
		ds_summation = value;
	} 
	
	public static Double getSummation() {
		return ds_summation;
	}
	
	public static void setSum(Double value) {
		ds_sum = value;
	}
	
	public static Double getSum() {
		return ds_sum;
	}	
	
	public static void setCount(Double value) {
		ds_count = value;
	}
	
	public static void addCount(Double value) {
		ds_count += value;
	}
	
	public static Double getCount() {
		return ds_count;
	}	
	
	public static void setAVG(Double value) {
		ds_avg = value;
	}	
	
	public static Double getAVG() {
		return ds_avg;
	}	
	
	public static void setStDev(Double value) {
		ds_stDev = value;
	}	
	
	public static Double getStDev() {
		return ds_stDev;
	}
	
	public static ArrayList<Double> normalize() {		
		if(ds_stDev < norm_threshold) {			
			return timeSerie;
		}
		// else, Normalize TimeSerie
		for(int i=0; i < timeSerie.size(); i++) {
			double element = (timeSerie.get(i) - ds_avg)/ds_stDev;
			timeSerie.add(i, element);
		}
		return timeSerie;		
	}	
	
	public String toString() {
		return "ds_count: " + SAXCalculi.getCount() + " | ds_sum: " + SAXCalculi.getSum() + " | ds_avg: " + SAXCalculi.getAVG() + " | stDev: " + SAXCalculi.getStDev();				 
		
	}
}
