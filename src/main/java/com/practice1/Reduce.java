package com.practice1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.util.*;
import java.io.IOException;
import java.util.Map;

public class Reduce implements Reducer<Text, Text, Text, Text> {

    UtilClass utilClass = new UtilClass();
    String result = "";
    Text theKey = null;

    @Override
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {

        //variant 1
        HashMap<Text,Integer> m = new HashMap<>();
        //Populate the HashMap
        while(values.hasNext()){
            String value = utilClass.getPeriod(values.next().toString());
            Text element = new Text(value);
            if(m.get(element) == null)
            {
                m.put(element , 1);
            }
            else m.put(element , m.get(element) + 1);
        }

        //Display the frequencies
        for (Map.Entry<Text, Integer> entry : m.entrySet()){
            if(theKey != key){
                result = "";
                theKey = key;
            }
            result += entry.getValue() + "_" + entry.getKey() + " ";
        }

        outputCollector.collect(key, new Text(result));
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void configure(JobConf jobConf) {

    }
}
