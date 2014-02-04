package com.rossfairbanks.elasticrawl.examples;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/*
 * A Hadoop custom mapper that simply collects each record it receives.
 * 
 * This allows the outputs from multiple Common Crawl segments to be 
 * reduced into a single set of results.
 * 
 * @author Ross Fairbanks
 */
public class SegmentCombinerMapper extends MapReduceBase 
	implements Mapper<Text, LongWritable, Text, LongWritable> {
	
	private static final String COUNTER_GROUP = "Combiner Mapper Counters";	
	private static final String RECORDS_FETCHED = "Records Fetched";

	/*
	 * Map method collects each record it receives.
	 */
	public void map(Text key, LongWritable value, OutputCollector<Text, LongWritable> output, Reporter reporter)
	        throws IOException {
		output.collect(key, value);
		reporter.incrCounter(COUNTER_GROUP, RECORDS_FETCHED, 1);
	}
}
