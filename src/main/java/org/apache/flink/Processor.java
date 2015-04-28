package org.apache.flink;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.xml.crypto.Data;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple13;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.util.Collector;

public class Processor {
	StreamExecutionEnvironment env;	
	private static SAXCalculi sax_obj = new SAXCalculi();
	DataStream<Double> timeSeries;
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
		p.env.execute("Giacomo");
	}
	
	private void startStreamingCollector() {
		// Setup Window parameters
		// WindowedDataStream<Double> SAXWindow
		this.window = timeSeries
				.window(Time.of(3, TimeUnit.SECONDS))
				.every(Time.of(3, TimeUnit.SECONDS));		
	}
	
	private void setup() throws Exception {					
		SAXCalculi sax = new SAXCalculi();
		// Calcola somme e count
		this.window.reduceGroup(new CalculiReducer());	
		this.window.reduceGroup(new MeanReducer()).print();		
		
	}		
	
	private ArrayList<Double> norm(DataSet<Double> ds) {
		// calcolare media(ds)
		// calcolare deviazione_standard(ds)
		
		ds.iterate(0).map(new MapFunction<Double, Double>() {

			@Override
			public Double map(Double value) throws Exception {
				/*
				 * 		    for (int i = 0; i < res.length; i++) {
		      res[i] = (series[i] - mean) / sd;
		    }
				 */
				return null;
			}
			
		});
		return null;
		
	}
	
	// TODO
	private Double stDev(DataSet<Double> ds){
		return 0.0;
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
}
