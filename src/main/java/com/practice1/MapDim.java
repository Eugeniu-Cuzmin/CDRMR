package com.practice1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapDim extends Mapper<WritableComparable, Text, Text, Text>{

    @Override
    public void map(WritableComparable key, Text value, Context context) throws IOException, InterruptedException {
        //split string
        String[] row = value.toString().split("\u0001");

        Text keyString = new Text(row[1]);
        Text valueString = new Text(row[0] + " " + row[3] + " " + row[4] + " " + row[7]);

//        System.out.println(valueString);

        context.write(keyString, valueString);
    }
}
