package com.practice1;

import org.apache.commons.math3.stat.inference.TestUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Reducer;
import ru.at_consulting.bigdata.secondary_sort.ComparedKey;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Reduce extends Reducer<ComparedKey, Text, Text, Text> {
    UtilClass utilClass = new UtilClass();
    Text theKey = null;
    LongWritable longWritableCdr = new LongWritable(1);
    LongWritable longWritableDir = new LongWritable(1);

    @Override
    public void reduce(ComparedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String string = "";
        String result = "";

        HashMap<Text, Integer> m = new HashMap<>();
        //Populate the HashMap
        for (Text value : values) {
            String[] stringValue = value.toString().split(" ");
            if(key.getComparedState().equals(longWritableCdr)){
                Text element = new Text(utilClass.getPeriod(stringValue[0] + " " + stringValue[1]));
                if (m.get(element) == null) {
                    m.put(element, 1);
                } else {
                    m.put(element, m.get(element) + 1);
                }
            }
            else{
                if(UtilClass.foundString == 0){
                    string = utilClass.findString(stringValue);
                }
            }
        }
        if(key.getComparedState().equals(longWritableCdr)){
            //Display the frequencies
            for (Map.Entry<Text, Integer> entry : m.entrySet()) {
                if (theKey != key.getKey()) {
                    result = "";
                    theKey = key.getKey();
                }
                result += entry.getValue() + "_" + entry.getKey() + " ";
            }
            utilClass.foundString = 0;
            System.out.println(key);
            context.write(key.getKey(), new Text(string + result));
        }

    }
}
