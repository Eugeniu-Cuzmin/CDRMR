package com.practice1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.at_consulting.bigdata.secondary_sort.ComparedKey;

import java.io.IOException;

public class Reduce extends Reducer<ComparedKey, Text, Text, Text> {
    private static String result = "";

    UtilClass utilClass = new UtilClass();

    LongWritable longWritableCdr = new LongWritable(0);
    LongWritable longWritableDir = new LongWritable(1);

    @Override
    public void reduce(ComparedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String string = "";
        //if cdr, count number of calls
        if(key.getComparedState().equals(longWritableCdr)){
            result = utilClass.findNumberOfCalls(values, key);
        }
        //if dir add values to result
        if(key.getComparedState().equals(longWritableDir) && result.length()>0){
            for (Text value : values) {
                string = value.toString();
            }
        }

        if(key.getComparedState().equals(longWritableDir) && result.length()>0){
            context.write(key.getKey(), new Text(string + " " + result));
            result = "";
        }
    }
}
