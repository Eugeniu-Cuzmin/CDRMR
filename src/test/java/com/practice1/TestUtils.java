package com.practice1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.WritableComparable;


public class TestUtils {
    public static final String KEY_VALUE_SEPARATOR = "\t";

    public static void loadInputText(String inputPath, MapReduceDriver mapReduceDriver) throws IOException {
        WritableComparable KEY = new Text();
        BufferedReader inputReader = new BufferedReader(new FileReader(inputPath));
        List<Pair<WritableComparable, Text>> input = new ArrayList<>();
        String line;
        while ((line = inputReader.readLine()) != null) {
            input.add(new Pair<>(KEY, new Text(line)));
        }
        inputReader.close();
        mapReduceDriver.addAll(input);
    }

    public static void sequenceToCsv(List<Pair<Text, Text>> pairs, String path) throws IOException {

        PrintWriter pw = new PrintWriter(path);
        for (Pair<?, ?> p : pairs) {
            pw.write(p.getFirst().toString() + KEY_VALUE_SEPARATOR + p.getSecond().toString() + "\n");
        }
        pw.flush();
        pw.close();
    }

}
