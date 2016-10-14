package com.practice1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Reduce extends Reducer<Text, Text, Text, Text> {

    UtilClass utilClass = new UtilClass();
    String result = "";
    Text theKey = null;

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        HashMap<Text,Integer> m = new HashMap<>();
        //Populate the HashMap
        for(Text value: values){
            Text element = new Text(utilClass.getPeriod(value.toString()));
            if(m.get(element) == null){
                m.put(element , 1);
            }else m.put(element , m.get(element) + 1);
        }

        //Display the frequencies
        for (Map.Entry<Text, Integer> entry : m.entrySet()){
            if(theKey != key){
                result = "";
                theKey = key;
            }
            result += entry.getValue() + "_" + entry.getKey() + " ";
        }
        context.write(key, new Text(result));
    }
}
