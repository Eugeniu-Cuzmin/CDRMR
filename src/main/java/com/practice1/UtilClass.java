package com.practice1;

import org.apache.hadoop.io.Text;
import ru.at_consulting.bigdata.secondary_sort.ComparedKey;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class UtilClass {

    private String[] split;
    public static int foundString = 0;

    public String getPeriod(String s) {
        return convertToString(s);
    }

    private String convertToString(String s) {
        DateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = null;

        try {
            date = format.parse(s);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        int hour = date.getHours();

        if(hour > 6 && hour <= 11){return "morning";}
        else if(hour > 11 && hour <= 18){return "day";}
        else if(hour > 18 && hour <= 23){return "evening";}
        else {return "night";}
    }


    public static String findString(String[] stringValue) {
        if(stringValue[3].equals("A") || stringValue[3].equals("S")){
            UtilClass.foundString = 1;
            return stringValue[0] + " " + stringValue[1] + " " + stringValue[2] + " ";
        }
        else{
            return stringValue[0] + " " + stringValue[1] + " " + stringValue[2] + " ";
        }
    }

    public String findNumberOfCalls(Iterable<Text> values, ComparedKey key) {
        HashMap<Text, Integer> m = new HashMap<>();
        Text theKey = null;
        String result = "";

        //Populate the HashMap
        for (Text value : values) {
            String[] stringValue = value.toString().split(" ");
            Text element = new Text(stringValue[0] + " " + stringValue[1]);

            if (m.get(element) == null) {
                m.put(element, 1);
            } else {
                m.put(element, m.get(element) + 1);
            }
        }

        //Display the frequencies
        for (java.util.Map.Entry<Text, Integer> entry : m.entrySet()) {
            if (theKey != key.getKey()) {
                result = "";
                theKey = key.getKey();
            }
            result += entry.getValue() + " " + entry.getKey() + " ";
        }
        return result;
    }
}
