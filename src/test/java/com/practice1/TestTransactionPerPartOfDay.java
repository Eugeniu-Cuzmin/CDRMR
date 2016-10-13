package com.practice1;

import com.practice1.stage1.MTransactionPerDay;
import com.practice1.stage1.RTransactionPerDay;
import com.prctice1.part2.MTransactionPerPartOfDay;
import com.prctice1.part2.RTransactionPerPartOfDay;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestTransactionPerPartOfDay {
    MapReduceDriver<WritableComparable, Text, Text, Text, Text, Text> mapReduceDriver;

    @Before
    public void setUp(){
        MTransactionPerPartOfDay mapper = new MTransactionPerPartOfDay();
        RTransactionPerPartOfDay reducer = new RTransactionPerPartOfDay();
        mapReduceDriver = new MapReduceDriver<WritableComparable, Text, Text, Text, Text, Text>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapReduce() {
        System.setProperty("hadoop.home.dir", "C:\\WorkSpace\\");

        mapReduceDriver.addInput(new LongWritable(1), new Text("0|9050000001|20160125204123"));
        mapReduceDriver.addInput(new LongWritable(1), new Text("00|905000|20160125204123"));
        mapReduceDriver.addInput(new LongWritable(1), new Text("0|9050000001|20160125204123"));
        mapReduceDriver.addInput(new LongWritable(1), new Text("00|9050000001|20160125134123"));
        mapReduceDriver.addInput(new LongWritable(1), new Text("11|9050000001|20160125134123"));

        mapReduceDriver.addOutput(new Text("905000"), new Text("morning 0, noon 0, evening 1, night 0"));
        mapReduceDriver.addOutput(new Text("9050000001"), new Text("morning 0, noon 2, evening 2, night 0"));

        mapReduceDriver.runTest();
    }
}
