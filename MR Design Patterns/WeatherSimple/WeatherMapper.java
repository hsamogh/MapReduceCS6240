package com.mapreduce;
/**
 *  This class implements mapper functionality . Implements mapper class
 *   Input Key: LongInt
 *   Input Value : Input Record from file (Text)
 *   Output Key : station-id
 *   Output Value :  com.mapreduce.WeatherRecord Object represented by com.mapreduce.WeatherRecord Class
 *
 */


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// Custom class to hold weather record

 class WeatherRecord implements Writable{
      public Text type;  //type can be TMAX or TMIN
      public DoubleWritable reading; // weather reading

     public WeatherRecord(){
         type = new Text();
         reading = new DoubleWritable();
     }

      public WeatherRecord(Text t, DoubleWritable r){
          type = t;
          reading = r;
      }

      public String getType(){
          return type.toString();
      }

      public double getReading(){
         return Double.parseDouble(reading.toString());
     }

     public void write(DataOutput dataOutput) throws IOException {

          type.write(dataOutput);
         reading.write(dataOutput);


     }

     public void readFields(DataInput dataInput) throws IOException {
          type.readFields(dataInput);
          reading.readFields(dataInput);

     }
 }

public class WeatherMapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,WeatherRecord> {

    //Implementing methods of interface
     public void map(LongWritable key, Text value, OutputCollector<Text, WeatherRecord> outputCollector, Reporter reporter)
            throws IOException {


        //the individual records from csv file is split based on ','
        String[] record = value.toString().split(",");

        //station-id is the first field in the file
        String stationId = record[0];

        //record-type(TMAX,TMIN,..) is the third field in the csv file
        String type = record[2];

        //temperature readings are fourth column in the csv file
        double temperature = Double.parseDouble(record[3]);

        //ignoring all records other than TMAX and TMIN
        if(type.equalsIgnoreCase("TMAX") || type.equalsIgnoreCase("TMIN")) {
            outputCollector.collect(new Text(stationId), (new WeatherRecord(new Text(type), new DoubleWritable(temperature))));
        }


    }
}
