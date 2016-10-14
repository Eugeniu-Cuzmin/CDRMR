package com.practice1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mrunit.MapReduceDriver;

public class TestUtils {
//    public static void loadInputText(String inputPath,
//                                     MapReduceDriver mapReduceDriver,
//                                     Mapper mapper) throws IOException {
//        WritableComparable KEY = new Text();
//        BufferedReader inputReader = new BufferedReader(new FileReader(inputPath));
//        List<Pair<WritableComparable, Text>> input = new ArrayList<>();
//        String line;
//        while ((line = inputReader.readLine()) != null) {
//            input.add(new Pair<>(KEY, new Text(line)));
//        }
//        inputReader.close();
//        mapReduceDriver.addAll(mapper, input);
//    }
}
