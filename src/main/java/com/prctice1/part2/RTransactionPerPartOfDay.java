package com.prctice1.part2;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Reducer;

public class RTransactionPerPartOfDay implements Reducer<Text, Text, Text, Text>{
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

        int transactionMorning = 0;
        int transactionNoon = 0;
        int transactionEvening = 0;
        int transactionNight= 0;

        while(values.hasNext()){
            String value = null;
            try {
                value = UtilClass.determinePartOfDay(values.next().toString());
            } catch (ParseException e) {
                e.printStackTrace();
            }

            if(value.equals("morning")) transactionMorning += 1;
            else if(value.equals("noon")) transactionNoon += 1;
            else if(value.equals("evening")) transactionEvening += 1;
            else if(value.equals("night")) transactionNight += 1;
        }

        String dayReduce = "morning " + transactionMorning
                + ", noon " + transactionNoon
                + ", evening " + transactionEvening
                + ", night " + transactionNight;

        outputCollector.collect(key, new Text(dayReduce));
    }

    public void close() throws IOException {

    }

    public void configure(JobConf jobConf) {

    }
}
