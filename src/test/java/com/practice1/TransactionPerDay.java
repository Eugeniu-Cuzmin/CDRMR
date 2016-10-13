package com.practice1;

import com.practice1.stage1.MTransactionPerDay;
import com.practice1.stage1.RTransactionPerDay;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TransactionPerDay {
    MapReduceDriver<WritableComparable, Text, Text, Text, Text, IntWritable> mapReduceDriver;


    @Before
    public void setUp(){
        MTransactionPerDay mapper = new MTransactionPerDay();
        RTransactionPerDay reducer = new RTransactionPerDay();
        mapReduceDriver = new MapReduceDriver<WritableComparable, Text, Text, Text, Text, IntWritable>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapReduce() {
        System.setProperty("hadoop.home.dir", "C:\\WorkSpace\\");
        mapReduceDriver.addInput(new LongWritable(1), new Text("0|9050000001|20160125204123"));
        mapReduceDriver.addInput(new LongWritable(1), new Text("0|9050000001|20160125204123"));
        mapReduceDriver.addInput(new LongWritable(1), new Text("0|9050000002|20160125204123"));
        mapReduceDriver.addOutput(new Text("9050000001"), new IntWritable(2));
        mapReduceDriver.addOutput(new Text("9050000002"), new IntWritable(1));
        mapReduceDriver.runTest();
    }
}
