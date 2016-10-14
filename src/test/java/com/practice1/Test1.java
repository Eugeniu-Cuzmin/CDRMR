package com.practice1;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class Test1 {
    MapReduceDriver<WritableComparable, Text, Text, Text, Text, Text> mapReduceDriver;
//    MultipleInputsMapReduceDriver driver;

    @Before
    public void setUp(){
        System.setProperty("hadoop.home.dir", "C:\\WorkSpace\\");
        Map mapper = new Map();
        Reduce reducer = new Reduce();
//        driver =  MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver();
//        driver.addMapper(mapper);
//        driver.setReducer(reducer);
        mapReduceDriver = new MapReduceDriver<WritableComparable, Text, Text, Text, Text, Text>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

//    @Test
//    public void testMR(){
//        driver.addInput(mapper<>, new LongWritable(1), new Text("0|9050000001|20160125204123|79644809999||HGMTC|1||22|79050000001|56321647569|||34110|I||||||250995210056537|354805064211510||56191|||38704||A|||11|V|81079681404134|5||||SE|||G|144|||||||||||||||Y|b00534589.huawei_anadyr.20151231184912||1|||||79681404134|0|||+@@+1{79098509982}2{2}3{2}5{79644809999}6{0000002A7A5AC635}7{79681404134}|20160125|"));
//    }

    @Test
    public void testMapReduce() {
        mapReduceDriver.addInput(new LongWritable(1), new Text("0|9050000001|20160125204123|79644809999||HGMTC|1||22|79050000001|56321647569|||34110|I||||||250995210056537|354805064211510||56191|||38704||A|||11|V|81079681404134|5||||SE|||G|144|||||||||||||||Y|b00534589.huawei_anadyr.20151231184912||1|||||79681404134|0|||+@@+1{79098509982}2{2}3{2}5{79644809999}6{0000002A7A5AC635}7{79681404134}|20160125|"));
        mapReduceDriver.addInput(new LongWritable(1), new Text("00|905000|20160125204123|79644809999||HGMTC|1||22|7905000|56321647569|||34110|I||||||250995210056537|354805064211510||56191|||38704||A|||11|V|81079681404134|5||||SE|||G|144|||||||||||||||Y|b00534589.huawei_anadyr.20151231184912||1|||||79681404134|0|||+@@+1{79098509982}2{2}3{2}5{79644809999}6{0000002A7A5AC635}7{79681404134}|20160125|"));
        mapReduceDriver.addInput(new LongWritable(1), new Text("0|9050000001|20160125204123|79644809999||HGMTC|1||22|79050000001|74720000000|||34110|I||||||250995210056537|354805064211510||56191|||38704||A|||11|V|81079681404134|5||||SE|||G|144|||||||||||||||Y|b00534589.huawei_anadyr.20151231184912||1|||||79681404134|0|||+@@+1{79098509982}2{2}3{2}5{79644809999}6{0000002A7A5AC635}7{79681404134}|20160125|"));
        mapReduceDriver.addInput(new LongWritable(1), new Text("00|9050000001|20160125134123|79644809999||HGSMT|2||0|0375293584296|79050000001|||34123|I||||||250995219022028|354724061501260||5083|||38706||A|||21|S|81079147991001|3||||EV|||G|144||||||||||||||||b00534589.huawei_anadyr.20151231184912||1||||||0||||20160125|"));
        mapReduceDriver.addInput(new LongWritable(1), new Text("11|9050000001|20160125134123|79644809999||HGSMT|1||0|79050000001|79050000111|||34123|I||||||250995219022028|354724061501260||5083|||38706||A|||21|S|81079147991001|3||||EV|||G|144||||||||||||||||b00534589.huawei_anadyr.20151231184912||1||||||0||||20160125|"));

        mapReduceDriver.addOutput(new Text("905000"), new Text("1_V_evening "));
        mapReduceDriver.addOutput(new Text("9050000001"), new Text("2_V_evening 2_S_day "));


        try {
            mapReduceDriver.runTest();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    @Test
//    public void testMapReduceWithInputFile() {
//        // Loading input paths
//        String filePath = "src\\test\\resources\\cdr.csv";
//
//        TestUtils.loadInputText(filePath, mapReduceDriver, MTransactionPerPartOfDay);
//        // Saving result
//        List<Pair<Text, Text>> output = mapReduceDriver.run();
//        TestUtils.sequenceToCsv(output, TestUtils.STG_1_OUTPUT);
//    }
}
