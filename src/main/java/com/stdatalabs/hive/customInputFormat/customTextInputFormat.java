package com.stdatalabs.hive.customInputFormat;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;

import com.stdatalabs.hive.customInputFormat.customRecordReader;;

/**
 * A custom input format for dealing with skipping records with specific length.
 * 
 * More discussion at stdatalabs.blogspot.com
 * 
 * @author Sachin Thirumala
 */

class CustomTextInputFormat extends TextInputFormat {

	@Override
	public RecordReader<LongWritable, Text> getRecordReader(InputSplit inputSplit, JobConf job, Reporter reporter)
			throws IOException {
		return new customRecordReader((FileSplit) inputSplit, job);
	}
}