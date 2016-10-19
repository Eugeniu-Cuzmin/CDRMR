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
    private static String result = "";
    private static String string = "";
    private static int join = 0;

    UtilClass utilClass = new UtilClass();

    LongWritable longWritableCdr = new LongWritable(0);
    LongWritable longWritableDir = new LongWritable(1);

    @Override
    public void reduce(ComparedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //if cdr, count number of calls
        if(key.getComparedState().equals(longWritableCdr)){
            result = utilClass.findNumberOfCalls(values, key);
            join = 0;
        }
        //if dir and A/S string not found, find add values to result
        if(key.getComparedState().equals(longWritableDir)){
            for (Text value : values) {
                string = value.toString();
            }
        }

        if(key.getComparedState().equals(longWritableDir) && result.length()>0){
            context.write(key.getKey(), new Text(string + result));
            result = "";
            string = "";
        }
    }
}
