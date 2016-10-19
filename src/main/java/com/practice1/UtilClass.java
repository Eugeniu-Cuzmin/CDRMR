package com.practice1;

import org.apache.hadoop.io.Text;
import ru.at_consulting.bigdata.secondary_sort.ComparedKey;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class UtilClass {

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
        //1 for morning, 2 for day, 3 for evening, 4 for night
        if(hour > 6 && hour <= 11){return "1";}
        else if(hour > 11 && hour <= 18){return "2";}
        else if(hour > 18 && hour <= 23){return "3";}
        else {return "4";}
    }


    public String findNumberOfCalls(Iterable<Text> values, ComparedKey key) {
        HashMap<Text, Integer> m = new HashMap<>();
        Text theKey = null;
        String result = "";

        //Populate the HashMap
        for (Text value : values) {
            String[] stringValue = value.toString().split(";");
            Text element = new Text(stringValue[0] + "_" + stringValue[1]);

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
            result += entry.getValue() + "_" + entry.getKey() + ";";
        }
        return result;
    }
}
