package com.practice1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import ru.at_consulting.bigdata.secondary_sort.ComparedKey;
import ru.at_consulting.bigdata.specs.Cdr;

import java.io.IOException;

public class MapDim extends Mapper<WritableComparable, Text, ComparedKey, Text>{

    private static final int BAN_KEY = 0;
    private static final int SUBS_KEY = 1;
    private static final int MARKET_KEY_SRC = 3;
    private static final int ACCOUNT_TYPE_KEY = 4;
    private static final int CURR_SUBS_STATUS_KEY = 7;

    TextTuple outKey = new TextTuple();
    TextTuple outValue = new TextTuple();
    String sortChar = "b";

    @Override
    public void map(WritableComparable key, Text value, Context context) throws IOException, InterruptedException {
        //split string
        String[] row = value.toString().split("\u0001");

        String keyDim = row[SUBS_KEY];
        String valueDim = row[BAN_KEY] + " " + row[MARKET_KEY_SRC] + " " + row[ACCOUNT_TYPE_KEY] + " " + row[CURR_SUBS_STATUS_KEY];

        ComparedKey comparedKey = new ComparedKey();
        comparedKey.setKey(keyDim);
        comparedKey.setComparedState(2);

        context.write(comparedKey, new Text(valueDim));
    }
}
