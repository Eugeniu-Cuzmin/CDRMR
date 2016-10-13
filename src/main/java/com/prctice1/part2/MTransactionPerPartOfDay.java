package com.prctice1.part2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MTransactionPerPartOfDay implements Mapper<WritableComparable, Text, Text, Text>{
    public void map(WritableComparable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
        //split string
        String[] row = value.toString().split("[|]");

        //define key/value pairs
        Text keyString = new Text(row[1]);
        Text valueString = new Text(row[2]);

        //result
        outputCollector.collect(keyString, valueString);
    }

    public void close() throws IOException {

    }

    public void configure(JobConf jobConf) {

    }
}
