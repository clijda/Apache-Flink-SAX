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

public class Processor {
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
	
	public Processor(StreamExecutionEnvironment p_env) {
		//this.streamEnv = p_env;		
		this.env = ExecutionEnvironment.getExecutionEnvironment();
		Processor.ds_avg = 0D;
		Processor.ds_sum = 0D;
		Processor.ds_stDev = 0D;
		Processor.ds_count = 0D;
		Processor.ds_summation = 0D;
	}	
	
	public static void main(String[] args) throws Exception {		
		Processor p = new Processor(StreamExecutionEnvironment.getExecutionEnvironment());		
		Utils u = new Utils();
		p.timeSeries2 = p.env.fromCollection(u.numbersGenerator(50000));
		p.setup();
		p.env.execute("Giacomo");
	}
	
	private void setup() throws Exception {					
		this.timeSeries2.reduceGroup(new CalculiReducer()).print();
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
					// Add value to SAXCalculi.timeSerie
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
						
				Processor.sax_obj.setAVG(ds_avg);
				Processor.sax_obj.setCount(ds_count);
				Processor.sax_obj.setSum(SAXCalculi.getSum());
				Processor.sax_obj.setSummation(SAXCalculi.getSummation());
				Processor.sax_obj.setStDev(SAXCalculi.getStDev());
				
				// Calculate SAX
				NormalAlphabet na = new NormalAlphabet();
				NumerosityReductionStrategy nrs = null;
				SAXProcessor sp = new SAXProcessor();
				SAXRecords SAXresult = sp.ts2saxViaWindow(SAXCalculi.normalize(), 120, 7, na.getCuts(5), nrs.fromValue(1), 1);
				
				Set<Integer> index = SAXresult.getIndexes();
				for (Integer idx : index) {
					String s = idx + COMMA + String.valueOf(SAXresult.getByIndex(idx).getPayload());
					out.collect(s);
				}												
			}		    	
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
}
