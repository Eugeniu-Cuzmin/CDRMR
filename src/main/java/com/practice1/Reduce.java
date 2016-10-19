package com.practice1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import ru.at_consulting.bigdata.secondary_sort.ComparedKey;

import java.io.IOException;

public class Reduce extends Reducer<ComparedKey, Text, Text, Text> {
    private static String result = "";
    private static String string = "";

    static Text keyText = new Text();

    UtilClass utilClass = new UtilClass();

    LongWritable longWritableDim = new LongWritable(0);
    LongWritable longWritableCdr = new LongWritable(1);

    @Override
    public void reduce(ComparedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //if dir add values to result
        if(key.getComparedState().equals(longWritableDim)){
            for (Text value : values) {
                string = value.toString();
            }
            keyText = key.getKey();
        }

        //if cdr, count number of calls
        if(key.getComparedState().equals(longWritableCdr)){
            result = utilClass.findNumberOfCalls(values, key);
            if(key.getKey().equals(keyText)){
                context.write(key.getKey(), new Text(";" + string + ";" + result));
                System.out.println(key.getKey().toString());
                System.out.println(key.getKey().toString().length());
                string = "";
            }
            else{
                context.write(key.getKey(), new Text(";" + result));
            }
        }
    }
}
