package com.practice1;

import org.apache.commons.math3.stat.inference.TestUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Reduce extends Reducer<TextTuple, TextTuple, Text, Text> {
    UtilClass utilClass = new UtilClass();
    Text theKey = null;

    @Override
    public void reduce(TextTuple key, Iterable<TextTuple> values, Context context) throws IOException, InterruptedException {
        String string = "";
        String result = "";

            HashMap<Text, Integer> m = new HashMap<>();

            //Populate the HashMap
            for (TextTuple value : values) {
                //check if left table
                String[] stringValue = value.toString().split(" ");
                if (stringValue[0].equals("cdr")) {
                    Text element = new Text(utilClass.getPeriod(stringValue[1] + " " + stringValue[2]));
                    if (m.get(element) == null) {
                        m.put(element, 1);
                    } else {
                        m.put(element, m.get(element) + 1);
                    }
                }
                else if(UtilClass.foundString == 0){
                    string = utilClass.findString(stringValue);
                }
            }

            //Display the frequencies
            for (Map.Entry<Text, Integer> entry : m.entrySet()) {
                if (theKey != key) {
                    result = "";
                    theKey = key;
                }
                result += entry.getValue() + "_" + entry.getKey() + " ";
            }
            utilClass.foundString = 0;
            context.write(key, new Text(string + result));
    }
}
