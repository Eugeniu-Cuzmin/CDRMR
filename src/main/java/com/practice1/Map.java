package com.practice1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<WritableComparable, Text, TextTuple, TextTuple> {
    private static final int SUBSCRIBER_NO = 1;
    private static final int CHANNEL_SEIZURE_DATE_TIME = 2;
    private static final int SERVICE_TYPE = 32;

    TextTuple outKey = new TextTuple();
    TextTuple outValue = new TextTuple();
    String sortChar = "a";

    @Override
    public void map(WritableComparable key, Text value, Context context) throws IOException, InterruptedException {
        //split string
        String[] row = value.toString().split("[|]", -1);

        if (row[SERVICE_TYPE].equals("")){
            row[SERVICE_TYPE] = "empty";
        }

        //define key/value pairs
        String keyCdr = row[SUBSCRIBER_NO];
        String valueCdr = row[CHANNEL_SEIZURE_DATE_TIME] + " " + row[SERVICE_TYPE];

        outKey.set(keyCdr, sortChar);
        outValue.set("cdr", valueCdr);

        //result
        context.write(outKey, outValue);
    }
}
