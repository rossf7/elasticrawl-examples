package com.rossfairbanks.elasticrawl.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.cmu.lemurproject.WarcFileInputFormat;

/*
 * A Hadoop job launched by the elasticrawl parse command.
 * 
 * Implements the standard Word Count example using 
 * Common Crawl WET (WARC Encoded Text) files.  These contain
 * text extractions of the web pages in the Common Crawl corpus.
 * 
 *  @author Ross Fairbanks
 */
public class WordCount extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(WordCount.class);

	private static final String MAX_FILES_KEY = "elasticrawl.max.files";
	private static final int MAX_MAP_TASK_FAILURES_PERCENT = 5;

	/*
	 * Entry point that runs the Hadoop job.
	 * 
	 * @param args array of input arguments
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = 0;
		exitCode = ToolRunner.run(new WordCount(), args);
		System.exit(exitCode);
	}

	/*
	 * Runs the Hadoop job which takes in 3 arguments.
	 * 
	 * Input Path: 	S3 location of a Common Crawl segment.
	 * Output Path: S3 location for storing the segment results.
	 * Max Files: 	Optionally restricts how many Common Crawl files 
	 * 				are processed.
	 * 
	 * @param args array of input arguments
	 * 
	 * @return result code for the job
	 */
	public int run(String[] args) throws Exception {
		// Get current configuration.
		Configuration conf = getConf();

		// Parse command line arguments.
		String inputPath = args[0];
		String outputPath = args[1];

		// Set Max Files if specified otherwise parse the entire segment.
		String maxFiles = "";
		if (args.length == 3)
			maxFiles = args[2];

		conf.set(MAX_FILES_KEY, maxFiles);
		
		JobConf job = new JobConf(conf);

		// Set input path.
		if (inputPath.length() > 0) {
			LOG.info("Setting input path to " + inputPath);
		    FileInputFormat.addInputPath(job, new Path(inputPath));
		    FileInputFormat.setInputPathFilter(job, FileCountFilter.class);
		} else {
			System.err.println("No input path found.");
			return 1;	
		}

		// Set output path.
		if (outputPath.length() > 0) {
			LOG.info("Setting output path to " + outputPath);
			SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath));
			// Compress output to boost performance.
			SequenceFileOutputFormat.setCompressOutput(job, true);
			SequenceFileOutputFormat.getOutputCompressorClass(job, GzipCodec.class);
		} else {
			System.err.println("No output path found.");
			return 1;
		}

		// Load other classes from same jar as this class.
		job.setJarByClass(WordCount.class);

		// Input is in WARC file format.
		job.setInputFormat(WarcFileInputFormat.class);

		// Output to Hadoop sequence file format.
		job.setOutputFormat(SequenceFileOutputFormat.class);

		// Set the output data types.
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// Use custom mapper class.
		job.setMapperClass(WordCountMapper.class);

		// Use standard reducer class.
		job.setReducerClass(LongSumReducer.class);
		
	    // Allow 5 percent of map tasks to fail.
	    job.setMaxMapTaskFailuresPercent(MAX_MAP_TASK_FAILURES_PERCENT);

		if (JobClient.runJob(job).isSuccessful())
			return 0;
		else
			return 1;
	}
}