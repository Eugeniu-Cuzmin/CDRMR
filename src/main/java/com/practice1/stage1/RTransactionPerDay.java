package com.practice1.stage1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;

public class RTransactionPerDay implements Reducer<Text, Text, Text, IntWritable>{
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        IntWritable intWritable = new IntWritable();
        int transactionCount = 0;
        while(values.hasNext()){
            transactionCount += 1;
            values.next();
        }
        intWritable.set(transactionCount);
        outputCollector.collect(key, intWritable);
    }

    public void close() throws IOException {

    }

    public void configure(JobConf jobConf) {

    }
}
