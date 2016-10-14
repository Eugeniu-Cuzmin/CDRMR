package com.practice1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;

public class Map implements Mapper<WritableComparable, Text, Text, Text> {
    public static final int SUBSCRIBER_NO = 1;
    public static final int CHANNEL_SEIZURE_DATE_TIME = 2;
    public static final int SERVICE_TYPE = 32;

    @Override
    public void map(WritableComparable key, Text value, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

        //split string
        String[] row = value.toString().split("[|]");

        //define key/value pairs
        Text keyString = new Text(row[SUBSCRIBER_NO]);
        Text valueString = new Text(row[CHANNEL_SEIZURE_DATE_TIME] + " " + row[SERVICE_TYPE]);

        //result
        outputCollector.collect(keyString, valueString);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {

    }
}
