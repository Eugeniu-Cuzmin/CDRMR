package com.practice1;

import org.apache.hadoop.io.Text;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class UtilClass {

    private String[] split;

    public String getPeriod(String s) {
        split = s.split(" ");
        return split[1] + "_" + convertToString(split[0]);
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
}
