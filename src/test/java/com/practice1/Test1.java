package com.practice1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MultipleInputsMapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;
import ru.at_consulting.bigdata.secondary_sort.ComparedKey;
import ru.at_consulting.bigdata.secondary_sort.CompositeKeyComparator;
import ru.at_consulting.bigdata.secondary_sort.GroupingKeyComparator;

import java.io.IOException;
import java.util.List;

public class Test1 {
    Map mapper = new Map();
    MapDim mapperDim = new MapDim();
    MultipleInputsMapReduceDriver<ComparedKey, Text, Text, Text> driver;
    String filePath = "src\\test\\resources\\numbers.txt";

    @Before
    public void setUp(){
        System.setProperty("hadoop.home.dir", "C:\\WorkSpace\\");
        Reduce reducer = new Reduce();
        GroupingKeyComparator groupingKeyComparator = new GroupingKeyComparator();
        CompositeKeyComparator compositeKeyComparator = new CompositeKeyComparator();
        driver = MultipleInputsMapReduceDriver.newMultipleInputMapReduceDriver();
        driver.addMapper(mapper);
        driver.addMapper(mapperDim);
        driver.addCacheFile(filePath);

//        driver.setKeyGroupingComparator(compositeKeyComparator);
//        driver.setKeyOrderComparator(compositeKeyComparator);
//        driver.setKeyComparator(compositeKeyComparator);

        driver.setReducer(reducer);
    }

    @Test
    public void testDriver() throws IOException {
        //paths
        String inputPath1 = "src\\test\\resources\\cdr.csv";
        String inputPath2 = "src\\test\\resources\\DIM_SUBSCRIBER.csv";
        String outputPath = "src\\test\\resources\\out\\OUTPUT2.csv";

        TestUtils.loadInputText(inputPath1, inputPath2, driver, mapper, mapperDim);

        List<Pair<Text, Text>> output = driver.run();
        TestUtils.sequenceToCsv(output, outputPath);
    }
}
