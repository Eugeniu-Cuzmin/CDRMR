package com.practice1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<WritableComparable, Text, Text, Text> {
    public static final int SUBSCRIBER_NO = 1;
    public static final int CHANNEL_SEIZURE_DATE_TIME = 2;
    public static final int SERVICE_TYPE = 32;

    @Override
    public void map(WritableComparable key, Text value, Context context) throws IOException, InterruptedException {

        //split string
        String[] row = value.toString().split("[|]");

        //define key/value pairs
        Text keyString = new Text(row[SUBSCRIBER_NO]);
        Text valueString = new Text(row[CHANNEL_SEIZURE_DATE_TIME] + " " + row[SERVICE_TYPE]);

        //result
        context.write(keyString, valueString);
    }
}
