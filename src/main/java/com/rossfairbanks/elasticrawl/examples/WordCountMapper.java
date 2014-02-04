package com.rossfairbanks.elasticrawl.examples;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.google.common.collect.HashMultiset;

import edu.cmu.lemurproject.WarcRecord;
import edu.cmu.lemurproject.WritableWarcRecord;

/*
 * A Hadoop custom mapper that implements the standard Word Count example.
 * 
 * Parses Common Crawl WET (WARC Encoded Text) files.
 */
public class WordCountMapper extends MapReduceBase 
	implements Mapper<LongWritable, WritableWarcRecord, Text, LongWritable> {

	private static final String COUNTER_GROUP = "Parser Mapper Counters";	
	private static final String RECORDS_FETCHED = "Records Fetched";
	private static final String RECORDS_COLLECTED = "Records Collected";

	private static final String WARC_RECORD_TYPE = "WARC-Type";
	private static final String CONVERSION_RECORD = "conversion";
	
	public static final Log LOG = LogFactory.getLog(WordCountMapper.class);

	/*
	 * Map method processes conversion records. Each WarcRecord contains the
	 * text content of a web page in the Common Crawl.
	 */
	public void map(LongWritable key, WritableWarcRecord value,
			OutputCollector<Text,LongWritable> output, Reporter reporter) throws IOException {
		int recordCount = 0;

		// Get Warc record from the writable wrapper.
		WarcRecord record = value.getRecord();
		String recordType = record.getHeaderMetadataItem(WARC_RECORD_TYPE);
		
		if (recordType.equals(CONVERSION_RECORD)) {
			// Get extracted page text.
			String pageText = record.getContentUTF8();
		
			// Remove all punctuation.
			pageText = pageText.replaceAll("[^a-zA-Z0-9 ]", "");
			
			// Normalize whitespace to single spaces.
			pageText = pageText.replaceAll("\\s+", " ");

			// Count words using a multiset.
			HashMultiset<String> wordCounts = HashMultiset.create();
		
			for (String word: pageText.split(" ")) {
				wordCounts.add(word);
			}

			// Collect word counts.
			Text outputKey = new Text();
			LongWritable outputValue = new LongWritable();
			
			for (String word: wordCounts.elementSet()) {
				outputKey.set(word);
				outputValue.set(wordCounts.count(word));
				
				output.collect(outputKey, outputValue);
				recordCount++;
			}
		}
			
		reporter.incrCounter(COUNTER_GROUP, RECORDS_FETCHED, 1);
		reporter.incrCounter(COUNTER_GROUP, RECORDS_COLLECTED, recordCount);
	}
}