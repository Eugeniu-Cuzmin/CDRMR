package com.prctice1.part2;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class UtilClass {

    public static String determinePartOfDay(String s) throws ParseException {
        String string = s;
        DateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        Date date = format.parse(string);
        int hour = date.getHours();
        if(hour > 6 && hour <= 11){return "morning";}
        else if(hour > 11 && hour <= 18){return "noon";}
        else if(hour > 18 && hour <= 23){return "evening";}
        else {return "night";}
    }

//    public static void main(String[] args) throws ParseException {
//        System.out.println(determinePartOfDay("20160125244123"));
//    }
}