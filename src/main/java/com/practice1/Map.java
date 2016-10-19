package com.practice1;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import ru.at_consulting.bigdata.secondary_sort.ComparedKey;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class Map extends Mapper<WritableComparable, Text, ComparedKey, Text> {
    private static final int SUBSCRIBER_NO = 1;
    private static final int CHANNEL_SEIZURE_DATE_TIME = 2;
    private static final int SERVICE_TYPE = 32;



    private Set numbersFromCache = new HashSet();

    @Override
    public void setup(Context context){
        try{
            Path[] numbersFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            if(numbersFiles != null && numbersFiles.length > 0) {
                for(Path numberFile : numbersFiles) {
                    readFile(numberFile);
                }
            }
        } catch(IOException ex) {
            System.err.println("Exception in mapper setup: " + ex.getMessage());
        }
    }

    private void readFile(Path numberFile) {
        try{
            BufferedReader bufferedReader = new BufferedReader(new FileReader(numberFile.toString()));
            String number = null;
            while((number = bufferedReader.readLine()) != null) {
                numbersFromCache.add(number.toLowerCase());
            }
        } catch(IOException ex) {
            System.err.println("Exception while reading stop words file: " + ex.getMessage());
        }
    }


    @Override
    public void map(WritableComparable key, Text value, Context context) throws IOException, InterruptedException {
        UtilClass utilClass = new UtilClass();

        //split string
        String[] row = value.toString().split("[|]", -1);

        //define key/value pairs
        String keyCdr = row[SUBSCRIBER_NO];
        String valueCdr = utilClass.getPeriod(row[CHANNEL_SEIZURE_DATE_TIME]) + " " + row[SERVICE_TYPE];

        ComparedKey comparedKey = new ComparedKey();
        comparedKey.setKey(keyCdr);
        comparedKey.setComparedState(0);

        if(numbersFromCache.contains(row[SUBSCRIBER_NO])){
            context.write(comparedKey, new Text(valueCdr));
        }
//        context.write(comparedKey, new Text(valueCdr));
    }
}
