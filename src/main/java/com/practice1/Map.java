package com.practice1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import ru.at_consulting.bigdata.secondary_sort.ComparedKey;

import java.io.IOException;

public class Map extends Mapper<WritableComparable, Text, ComparedKey, Text> {
    private static final int SUBSCRIBER_NO = 1;
    private static final int CHANNEL_SEIZURE_DATE_TIME = 2;
    private static final int SERVICE_TYPE = 32;

    @Override
    public void map(WritableComparable key, Text value, Context context) throws IOException, InterruptedException {
        //split string
        String[] row = value.toString().split("[|]", -1);

        //define key/value pairs
        String keyCdr = row[SUBSCRIBER_NO];
        String valueCdr = row[CHANNEL_SEIZURE_DATE_TIME] + " " + row[SERVICE_TYPE];

        ComparedKey comparedKey = new ComparedKey();
        comparedKey.setKey(keyCdr);
        comparedKey.setComparedState(1);

        //result
        context.write(comparedKey, new Text(valueCdr));
    }
}
