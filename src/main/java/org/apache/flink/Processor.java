package org.apache.flink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.sax.SAXProcessor;
import org.apache.sax.NumerosityReductionStrategy;
import org.apache.sax.NormalAlphabet;
import org.apache.sax.SAXRecords;
import org.apache.sax.SaxRecord;

public class Processor {
	StreamExecutionEnvironment env;	
	private static SAXCalculi sax_obj = new SAXCalculi();
	DataStream<Double> timeSeries;
	private static ArrayList<Double> ds_list = new ArrayList<Double>();
	private static final String COMMA = ", ";
	WindowedDataStream<Double> window;
	
	public Processor(StreamExecutionEnvironment p_env) {
		this.env = p_env;		
	}	
	
	public static void main(String[] args) throws Exception {		
		Processor p = new Processor(StreamExecutionEnvironment.getExecutionEnvironment());
		p.timeSeries = p.env
				.socketTextStream("localhost", 8888)
				.flatMap(new Splitter());
		
		p.startStreamingCollector();
		p.setup();
		p.executeSAX();
		p.env.execute("Giacomo");
	}
	
	private void startStreamingCollector() {
		// Setup Window parameters
		// WindowedDataStream<Double> SAXWindow
		this.window = timeSeries
				.window(Time.of(10, TimeUnit.SECONDS))
				.every(Time.of(10, TimeUnit.SECONDS));		
	}
	
	private void setup() throws Exception {					
		//SAXCalculi sax = new SAXCalculi();
		// Calcola somme e count
		this.window.reduceGroup(new CalculiReducer());	
		this.window.reduceGroup(new MeanReducer());
		this.window.reduceGroup(new StDevReducer());		
		ArrayList<Double> NormDataSet = this.norm(this.window);
	}		
	
	private void executeSAX() throws Exception {	
		NormalAlphabet na = new NormalAlphabet();
		NumerosityReductionStrategy nrs = null;
		SAXProcessor sp = new SAXProcessor();
		SAXRecords SAXresult = sp.ts2saxViaWindow(ds_list, 120, 7, na.getCuts(5), nrs.fromValue(1), 0.1);
		
		  Set<Integer> index = SAXresult.getIndexes();
		  for (Integer idx : index) {
		    System.out.println(idx + COMMA + String.valueOf(SAXresult.getByIndex(idx).getPayload()));
		  }
		
		PrintStream fileStream;
		try {
		fileStream = new PrintStream(new File("Results/results.txt"));
		//fileStream.println("Array = [");
		for (int j = 0; j<SAXresult.size(); j++){
			fileStream.println(SAXresult.getByIndex(j));
		}
		//fileStream.println("]");
		fileStream.println();
		} catch (FileNotFoundException e) {
			// 	TODO Auto-generated catch block
			e.printStackTrace();
		}			
	}
	
	private ArrayList<Double> norm(WindowedDataStream<Double> ds) {
		// calcolare media(ds)
		// calcolare deviazione_standard(ds)
		double stDev =sax_obj.getStDev();
		double mean = sax_obj.getAVG();
		// Convert DataSet and add it to ds_list
		this.addDStoList(ds);
		ArrayList<Double> ds_norm = new ArrayList<Double>();
		
		if(stDev < sax_obj.norm_threshold) {			
			return ds_list;
		}
		for(int i=0; i < ds_list.size(); i++) {
			double element = (ds_list.get(i) - mean)/stDev;
			ds_norm.add(i, element);
		}
		
		return ds_norm;		
	}
	
	public void addDStoList(WindowedDataStream<Double> ds) {
		// This reduceGroup iterate over ds and add items to ds_list
		ds.reduceGroup(new dsConverter());		
	}
		
    public static class Splitter implements FlatMapFunction<String, Double> {
        @Override
        public void flatMap(String sentence, Collector<Double> out) throws Exception {
            for (String value: sentence.split("\\s+")) {
            	System.out.println("VALUE : " + value);
                out.collect(Double.parseDouble(value));
            }
        }
    }	
    
    public static class CalculiReducer implements GroupReduceFunction<Double, SAXCalculi> {

		@Override
		public void reduce(Iterable<Double> values, Collector<SAXCalculi> out)
				throws Exception {
			
			Iterator<Double> iterator = values.iterator();
			while(iterator.hasNext()) {
				double value = iterator.next();
				double sum = sax_obj.getSum();
				sax_obj.setSum(sum+value);
				sax_obj.addCount(1.0);
				out.collect(sax_obj);
			}
			
		}	
    	
    }
    
    public static class MeanReducer implements GroupReduceFunction<Double, SAXCalculi> {

		@Override
		public void reduce(Iterable<Double> values, Collector<SAXCalculi> out)
				throws Exception {
			
			double avg = sax_obj.getSum() / sax_obj.getCount();
			sax_obj.setAVG(avg);
			out.collect(sax_obj);						
		}	
    	
    }
    
    public static class StDevReducer implements GroupReduceFunction<Double, SAXCalculi> {
		@Override
		public void reduce(Iterable<Double> values, Collector<SAXCalculi> out)
				throws Exception {
			Iterator<Double> iterator = values.iterator();
			//double summation = 0D;
			double ds_avg = sax_obj.getAVG();
			double ds_count = sax_obj.getCount();			
			double ds_summation = sax_obj.getSummation();
			while(iterator.hasNext()) {
				double value = iterator.next();
				ds_summation += value*value;
			}
			
			// Calculate Standard Deviation
			double x = (ds_summation - (ds_avg*ds_avg)*ds_count) / (ds_count-1);
			double stDev = Math.sqrt(x);
			
			sax_obj.setSummation(ds_summation);
			sax_obj.setStDev(stDev);
			out.collect(sax_obj);
		}
    }
    
    
    public static class dsConverter implements GroupReduceFunction<Double, Double> {

		@Override
		public void reduce(Iterable<Double> values, Collector<Double> out)
				throws Exception {			
			// Clean ds_list
			ds_list.clear();		
			while(values.iterator().hasNext()) {
				double value = values.iterator().next();
				ds_list.add(value);
				out.collect(value);
			} 			
		}    	
    }
}
