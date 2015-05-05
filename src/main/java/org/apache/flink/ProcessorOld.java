package org.apache.flink;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.flink.util.Collector;
import org.apache.flink.utils.Utils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.shaded.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.sax.SAXProcessor;
import org.apache.sax.NumerosityReductionStrategy;
import org.apache.sax.NormalAlphabet;
import org.apache.sax.SAXRecords;
import org.apache.sax.SaxRecord;

public class ProcessorOld {
	StreamExecutionEnvironment streamEnv;
	ExecutionEnvironment env;
	private static SAXCalculi sax_obj = new SAXCalculi();
	DataStream<Double> timeSeries;
	DataSource<Double> timeSeries2;
	//private static ArrayList<Double> ds_list = new ArrayList<Double>();
	private static final String COMMA = ", ";
	WindowedDataStream<Double> window;
	DataSet<Double> chunk;
	static Double ds_avg;
	static Double ds_sum;
	static Double ds_count;
	static Double ds_stDev;
	static Double ds_summation;
	
	public ProcessorOld(StreamExecutionEnvironment p_env) {
		//this.streamEnv = p_env;		
		this.env = ExecutionEnvironment.getExecutionEnvironment();
		ProcessorOld.ds_avg = 0D;
		ProcessorOld.ds_sum = 0D;
		ProcessorOld.ds_stDev = 0D;
		ProcessorOld.ds_count = 0D;
		ProcessorOld.ds_summation = 0D;
	}	
	
	public static void main(String[] args) throws Exception {		
		ProcessorOld p = new ProcessorOld(StreamExecutionEnvironment.getExecutionEnvironment());
		
		/*p.timeSeries = p.streamEnv
				.socketTextStream("localhost", 9999)
				.flatMap(new Splitter());*/
		
		Utils u = new Utils();
		p.timeSeries2 = p.env.fromCollection(u.numbersGenerator(500));
		//p.timeSeries2.print();
		
		//p.startStreamingCollector();
		p.setup();
		//p.executeSAX();
		//p.streamEnv.execute("Giacomo");
		p.env.execute("Giacomo");
	}
	
	private void startStreamingCollector() {
		// Setup Window parameters
		// WindowedDataStream<Double> SAXWindow
		this.window = timeSeries
				.window(Time.of(5, TimeUnit.SECONDS))
				.every(Time.of(10, TimeUnit.SECONDS));		
	}
	
	private void setup() throws Exception {					
		//SAXCalculi sax = new SAXCalculi();
		// Calcola somme e count
		/*this.window.reduceGroup(new CalculiReducer());				
		this.window.reduceGroup(new MeanReducer());
		this.window.reduceGroup(new StDevReducer());
		this.norm(this.window);		*/
		this.timeSeries2.reduceGroup(new CalculiReducer()).print();
		//this.timeSeries2.reduceGroup(new executeSAX2()).print();
		//this.timeSeries2.reduceGroup(new MeanReducer()).print();		
		//this.timeSeries2.reduceGroup(new StDevReducer()).print();
		
		//this.timeSeries2.reduceGroup(new MeanReducer()).print();
		//this.timeSeries2.reduceGroup(new StDevReducer());
		//this.normChunk(this.timeSeries2);
		//DataSet<Double> tm = this.timeSeries2.reduceGroup(new normalizer());
		//tm.print();
		//tm.reduceGroup(new executeSAX2()).print();
		//if(sax_obj.timeSerie.size() > 0) {
			//this.timeSeries2.print();
			//DataSource<Double> ds = this.env.fromCollection(sax_obj.timeSerie);		
			//this.timeSeries2.reduceGroup(new executeSAX2()).print();
		//}
	}		
	
	   public static class CalculiReducer implements GroupReduceFunction<Double, String> {

			@Override
			public void reduce(Iterable<Double> values, Collector<String> out)
					throws Exception {
				
				double x = 0D;
				double stDev = 0D;
				
				Iterator<Double> iterator = values.iterator();
				while(iterator.hasNext()) {
					double value = iterator.next();
					double sum = SAXCalculi.getSum();
					// Add value tu SAXCalculi.timeSerie
					SAXCalculi.timeSerie.add(value);
					SAXCalculi.setSum(sum+value);
					SAXCalculi.addCount(1.0);		
					
					
					// Update AVG
					ds_avg = SAXCalculi.getSum() / SAXCalculi.getCount();
					SAXCalculi.setAVG(ds_avg);
					
					// Update STANDARD DEVIATION		
					ds_summation += value*value;
					ds_count = SAXCalculi.getCount();
					x = (ds_summation - (ds_avg*ds_avg)*ds_count) / (ds_count-1);
					stDev = Math.sqrt(x);					
					SAXCalculi.setSummation(ds_summation);
					SAXCalculi.setStDev(stDev);					
				}
				

				
				System.out.println("#### DS SUM: " + ds_summation);		
				ProcessorOld.sax_obj.setAVG(ds_avg);
				ProcessorOld.sax_obj.setCount(ds_count);
				ProcessorOld.sax_obj.setSum(SAXCalculi.getSum());
				ProcessorOld.sax_obj.setSummation(SAXCalculi.getSummation());
				ProcessorOld.sax_obj.setStDev(SAXCalculi.getStDev());
				
				// Calculate SAX
				NormalAlphabet na = new NormalAlphabet();
				NumerosityReductionStrategy nrs = null;
				SAXProcessor sp = new SAXProcessor();
				System.out.println("##### EXECUTE SAX #####");	
				System.out.println(SAXCalculi.timeSerie);
				SAXRecords SAXresult = sp.ts2saxViaWindow(SAXCalculi.timeSerie, 120, 7, na.getCuts(5), nrs.fromValue(1), 1);
				
				Set<Integer> index = SAXresult.getIndexes();
				for (Integer idx : index) {
					String s = idx + COMMA + String.valueOf(SAXresult.getByIndex(idx).getPayload());
					//System.out.println(s);
					out.collect(s);
				}					
				
				//out.collect(Processor.sax_obj);				
			}		    	
	    }
	   
		public static class executeSAX2 implements GroupReduceFunction<Double, String> {
			
			@Override
			public void reduce(Iterable<Double> values, Collector<String> out)
					throws Exception {
				NormalAlphabet na = new NormalAlphabet();
				NumerosityReductionStrategy nrs = null;
				SAXProcessor sp = new SAXProcessor();
				System.out.println("##### EXECUTE SAX #####");	
				System.out.println(sax_obj.timeSerie);
				SAXRecords SAXresult = sp.ts2saxViaWindow(sax_obj.timeSerie, 120, 7, na.getCuts(5), nrs.fromValue(1), 1);
				
				Set<Integer> index = SAXresult.getIndexes();
				for (Integer idx : index) {
					String s = idx + COMMA + String.valueOf(SAXresult.getByIndex(idx).getPayload());
					//System.out.println();
					out.collect(s);
				}					
			}
		}	   
	    
	    public static class MeanReducer implements GroupReduceFunction<Double, SAXCalculi> {

			@Override
			public void reduce(Iterable<Double> values, Collector<SAXCalculi> out)
					throws Exception {				
				double avg = SAXCalculi.getSum() / SAXCalculi.getCount();
				SAXCalculi.setAVG(avg);
				
				System.out.println("##### AVG > " + SAXCalculi.getAVG());
				out.collect(sax_obj);						
			}	
	    	
	    }
	    
	    public static class StDevReducer implements GroupReduceFunction<Double, SAXCalculi> {
			@Override
			public void reduce(Iterable<Double> values, Collector<SAXCalculi> out)
					throws Exception {
				Iterator<Double> iterator = values.iterator();
				//double summation = 0D;
				double ds_avg = ProcessorOld.sax_obj.getAVG();
				double ds_count = ProcessorOld.sax_obj.getCount();			
				double ds_summation = ProcessorOld.sax_obj.getSummation();
				
				while(iterator.hasNext()) {
					double value = iterator.next();
					ds_summation += value*value;				
					System.out.println("#### DS SUM" + ds_summation);
				}
				
				System.out.println("#### DS SUM LAST: " + ds_summation);
				// Calculate Standard Deviation
				double x = (ds_summation - (ds_avg*ds_avg)*ds_count) / (ds_count-1);
				double stDev = Math.sqrt(x);
				
				ProcessorOld.sax_obj.setSummation(ds_summation);
				ProcessorOld.sax_obj.setStDev(stDev);
				System.out.println("#### stDev >" + ProcessorOld.sax_obj.getStDev());
				System.out.println("#### DS SUM obj: " + ProcessorOld.sax_obj.getSummation());
				out.collect(ProcessorOld.sax_obj);			
			}
	    }	
	
	
	private void executeSAX() throws Exception {	
		NormalAlphabet na = new NormalAlphabet();
		NumerosityReductionStrategy nrs = null;
		SAXProcessor sp = new SAXProcessor();
		System.out.println("EXECUTE SAX");		
		SAXRecords SAXresult = sp.ts2saxViaWindow(sax_obj.timeSerie, 120, 7, na.getCuts(5), nrs.fromValue(1), 1);
		
		  Set<Integer> index = SAXresult.getIndexes();
		  for (Integer idx : index) {
		    System.out.println(idx + COMMA + String.valueOf(SAXresult.getByIndex(idx).getPayload()));
		  }
		
		/*PrintStream fileStream;
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
		}		*/	
	}
	
	private ArrayList<Double> normChunk(DataSet<Double> ds) {
		System.out.println("### NORM ###");
		double stDev =sax_obj.getStDev();
		double mean = sax_obj.getAVG();
		// Convert DataSet and add it to ds_list
		this.addDStoList2(ds);
		//ArrayList<Double> ds_norm = new ArrayList<Double>();
		
		if(stDev < sax_obj.norm_threshold) {			
			return sax_obj.timeSerie;
		}
		// else, Normalize TimeSerie
		for(int i=0; i < sax_obj.timeSerie.size(); i++) {
			double element = (sax_obj.timeSerie.get(i) - mean)/stDev;
			sax_obj.timeSerie.add(i, element);
		}
		return sax_obj.timeSerie;		
	}

	private ArrayList<Double> norm(WindowedDataStream<Double> ds) {
		//System.out.println("### NORM ###");
		double stDev =sax_obj.getStDev();
		double mean = sax_obj.getAVG();
		// Convert DataSet and add it to ds_list
		this.addDStoList(ds);
		//ArrayList<Double> ds_norm = new ArrayList<Double>();
		
		if(stDev < sax_obj.norm_threshold) {			
			return sax_obj.timeSerie;
		}
		// else, Normalize TimeSerie
		for(int i=0; i < sax_obj.timeSerie.size(); i++) {
			double element = (sax_obj.timeSerie.get(i) - mean)/stDev;
			sax_obj.timeSerie.add(i, element);
		}
		return sax_obj.timeSerie;		
	}
	
	
	public void addDStoList(WindowedDataStream<Double> ds) {
		// This reduceGroup iterate over ds and add items to ds_list
		ds.reduceGroup(new dsConverter());		
	}
	
	public void addDStoList2(DataSet<Double> ds) {
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
    
 
    
    
    public static class dsConverter implements GroupReduceFunction<Double, Double> {

		@Override
		public void reduce(Iterable<Double> values, Collector<Double> out)
				throws Exception {			
			// Clean timeSerie
			sax_obj.timeSerie.clear();
			sax_obj.timeSerie = Lists.newArrayList(values);
			/*while(values.iterator().hasNext()) {
				double value = values.iterator().next();
				ds_list.add(value);
				out.collect(value);
			}*/ 			
		}    	
    }
    
    public static class normalizer implements GroupReduceFunction<Double, Double> {

		@Override
		public void reduce(Iterable<Double> values, Collector<Double> out)
				throws Exception {

			System.out.println("### NORM2 ###");
			double stDev =sax_obj.getStDev();
			double mean = sax_obj.getAVG();
			//ArrayList<Double> ds_norm = new ArrayList<Double>();
			
			
			while(values.iterator().hasNext()) {
				Double value = values.iterator().next();
				if(stDev >= sax_obj.norm_threshold) {
					double element = (value - mean)/stDev;
					out.collect(element);
				}
				
				
			}
				// else, Normalize TimeSerie
				/*for(int i=0; i < sax_obj.timeSerie.size(); i++) {
					double element = (sax_obj.timeSerie.get(i) - mean)/stDev;
					sax_obj.timeSerie.add(i, element);
				}*/	
		}	
    }
}
