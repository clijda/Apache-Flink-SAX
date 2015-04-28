package org.apache.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SAXCalculi {
	
	private double ds_sum;
	private double ds_count;
	private double ds_avg;
	private double ds_stDev;
	
	public SAXCalculi() {
		this.ds_sum = 0D;
		this.ds_count = 0D;
		this.ds_avg = 0D;
		this.ds_stDev = 0D;
	}
	
	public void setSum(Double value) {
		this.ds_sum = value;
	}
	
	public Double getSum() {
		return this.ds_sum;
	}	
	
	public void setCount(Double value) {
		this.ds_count = value;
	}
	
	public void addCount(Double value) {
		this.ds_count += value;
	}
	
	public Double getCount() {
		return this.ds_count;
	}	
	
	public void setAVG(Double value) {
		this.ds_avg = value;
	}	
	
	public Double getAVG() {
		return this.ds_avg;
	}	
	
	public void setStDev(Double value) {
		this.ds_stDev = value;
	}	
	
	public Double getStDev() {
		return this.ds_stDev;
	}	
	
	public String toString() {
		return "ds_count: " + this.getCount() + " | ds_sum: " + this.getSum() + " | ds_avg: " + this.getAVG();
		
	}
}
