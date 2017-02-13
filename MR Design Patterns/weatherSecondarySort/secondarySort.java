package com.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class secondarySort  extends Configured implements Tool{

    /**
     * This class holds the components of value that needs to be passed from
     * mapper to reducer
     */

    public static class WeatherRecord implements Writable{

        double maxTempSum;
        double minTempSum;
        int maxTempCount;
        int minTempCount;
        String year;

        //default constructor
        public WeatherRecord(){
            maxTempSum = 0;
            minTempSum = 0;
            maxTempCount = 0;
            minTempCount = 0;
            year =  "";
        }

        // constructor
        public WeatherRecord(double maxTempSum,
                              int maxTempCount,
                              double minTempSum,
                              int minTempCount,
                              String year){

            this.maxTempSum = maxTempSum;
            this.maxTempCount = maxTempCount;
            this.minTempSum = minTempSum;
            this.minTempCount = minTempCount;
            this.year = year;
        }

       // function to serialize data
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeDouble(maxTempSum);
            dataOutput.writeDouble(minTempSum);
            dataOutput.writeInt(maxTempCount);
            dataOutput.writeInt(minTempCount);
            dataOutput.writeUTF(year);
        }

        //function to deserialize data
        public void readFields(DataInput dataInput) throws IOException {

            maxTempSum = dataInput.readDouble();
            minTempSum = dataInput.readDouble();
            maxTempCount = dataInput.readInt();
            minTempCount = dataInput.readInt();
            year = dataInput.readUTF();

        }
    } //end of WeatherRecord Class

    /**
     *  Function to implement custom combiner.
     */

    public static class WeatherCustomCombiner extends Reducer<MapEmitKey,
                                                 WeatherRecord,MapEmitKey,WeatherRecord>{

        protected void reduce(MapEmitKey key,
                              Iterable<WeatherRecord>  values,
                              Context context)throws IOException,
                                                    InterruptedException {

            double maxTemp = 0;
            int maxTempCount = 0;
            double minTemp = 0;
            int minTempCount = 0;
            String year = null;
            //Iterating over values and aggregating data
            for (WeatherRecord w : values){

                maxTemp += w.maxTempSum;
                maxTempCount += w.maxTempCount;
                minTemp += w.minTempSum;
                minTempCount += w.minTempCount;
                year = w.year;

            }
            //emitting aggregate data
            context.write(key, new WeatherRecord( maxTemp,
                                                  maxTempCount,
                                                  minTemp,
                                                  minTempCount,
                                                  year));
        }
    }

    /**
     * Class to define custom key . The key for our secondary sort is
     * (station-id,year). We are also overriding the comapreTo function
     * and defining our own custom comparator
     */


    public static class MapEmitKey implements Writable , WritableComparable{
        public String year;
        public String stationId;

        public MapEmitKey(){
            year="";
            stationId="";
        }

        public MapEmitKey(String stationId,String year){
            this.stationId = stationId;
            this.year = year;
        }


        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(year);
            dataOutput.writeUTF(stationId);
        }

        public void readFields(DataInput dataInput) throws IOException {
            year = dataInput.readUTF();
            stationId = dataInput.readUTF();
        }

        public int compareTo(Object o) {
            MapEmitKey w = (MapEmitKey)o;
            int compareValue = this.stationId.compareTo(w.stationId);
            if(compareValue == 0)
                return this.year.compareTo(w.year);
            return compareValue;
        }
    } // end of MapEmitKey class

    /**
     *
     * Mapper Class
     *
     */

    public static class SSMap extends Mapper<Object,Text,MapEmitKey,WeatherRecord>{

        protected void map(Object key, Text values , Context context){

            String[] record = values.toString().split(",");

            //station-id is the first field in the record
            String stationId = record[0].trim();
            String  year = record[1].trim().substring(0,4);
            String  type = record[2].trim();

            if(type.equalsIgnoreCase("TMAX") || type.equalsIgnoreCase("TMIN")){

                String tempValue = record[3];
                MapEmitKey k = new MapEmitKey(stationId,year);

                if(type.equalsIgnoreCase("TMAX")){
                    try {

                            context.write(k,new WeatherRecord(Double.parseDouble(tempValue),
                                    1,0.0, 0,
                                     year));

                    } catch (Exception e) {
                            System.out.println("Error while emmiting map output"+ e);
                    }
                }
                else if(type.equalsIgnoreCase("TMIN")){
                    try {

                        context.write(k,new WeatherRecord(0.0,
                                0,Double.parseDouble(tempValue),
                                1,year));

                    } catch (Exception e) {
                        System.out.println("Error while emmiting map output"+ e);
                    }
                }
            }
        }
    }

    /**
     *
     *  Reducer Class
     *
     */

    public static class SSReduce extends Reducer<MapEmitKey,WeatherRecord,NullWritable,Text>{

        protected void reduce(MapEmitKey key, Iterable<WeatherRecord>  values,Context context) throws IOException, InterruptedException {

            double maxTemp = 0;
            int maxTempCount = 0;
            double minTemp = 0;
            int minTempCount = 0;
            String year = null;

            String outputString = key.stationId + ", [ ";


            for (WeatherRecord w : values){

                //context.write(NullWritable.get(),new Text(w.maxTempCount + " " + w.minTempCount + " "+ w.maxTempSum + " "+ w.maxTempCount));

                if(year!=null && !year.equals(w.year)){

                    outputString += "(" + year + ", ";
                    outputString += ( minTempCount > 0) ?(minTemp / minTempCount)+ "," : "None ,";
                    outputString += ( maxTempCount > 0) ?(maxTemp / maxTempCount)+ ",)" : "None ,)";
                    maxTemp = 0;
                    minTemp = 0;
                    maxTempCount = 0;
                    minTempCount = 0;
                }
                maxTemp += w.maxTempSum;
                maxTempCount += w.maxTempCount;
                minTemp += w.minTempSum;
                minTempCount += w.minTempCount;
                year = w.year;

            }
            outputString += "(" + year + ", ";
            outputString += ( minTempCount > 0) ?(minTemp / minTempCount)+ "," : "None ,";
            outputString += (maxTempCount > 0) ?(maxTemp / maxTempCount)+ ",)]" : "None ,)]";

            context.write(NullWritable.get(), new Text(outputString));



        }
    }


    /***
     *   Custom Grouing Comparator . Used to group all records having
     *   similar station-id
     */

    public static class WeatherGroupingComparator extends WritableComparator{

        public WeatherGroupingComparator(){
            super(MapEmitKey.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2){
            MapEmitKey key1 = (MapEmitKey)w1;
            MapEmitKey key2 = (MapEmitKey)w2;
            return (key1.stationId.compareTo(key2.stationId));
        }

    }


    /**
     *  Class which contains custom partitioner
     */

    public static class WeatherCustomPartitioner extends Partitioner
                                                    <MapEmitKey,WeatherRecord>{
        //@Override
        public int getPartition(MapEmitKey key,
                                WeatherRecord temp,
                                int reducers){
           // System.out.println(key.stationId.hashCode()%reducers);
            return ((key.stationId.hashCode()& Integer.MAX_VALUE)%reducers);
        }
    }

    /**
     *  The controlling function for mao reduce job
     *
     */

    public int run(String[] args) throws Exception {

        if(args.length <2){
            System.out.println("One or more of the command line parameters is missing");
            return -1;
        }

        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();


        Job job = new Job(conf, "secondary Sort");
        job.setJarByClass(secondarySort.class);
        //setting mapper class
        job.setMapperClass(SSMap.class);
        //setting reducer class
        job.setReducerClass(SSReduce.class);
        //setting combiner class
        job.setCombinerClass(WeatherCustomCombiner.class);
        //setting partitioner class
        job.setPartitionerClass(WeatherCustomPartitioner.class);
        //setting grouping Comparator
        job.setGroupingComparatorClass(WeatherGroupingComparator.class);

        job.setOutputKeyClass(MapEmitKey.class);
        job.setOutputValueClass(WeatherRecord.class);

        // adding input paths
        for(int i=0; i<args.length-1 ; i++){
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        // adding input paths
        Path outPath = new Path(args[args.length-1]);
        FileOutputFormat.setOutputPath(job, outPath);
        outPath.getFileSystem(conf).delete(outPath, true);

        job.waitForCompletion(true);
        return (job.waitForCompletion(true) ? 0 : 1);

    } // end of run function

   //Entry point of program
    public static void main(String[] args){

        int exitCode = 0;
        try {
            exitCode = ToolRunner.run(new secondarySort(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.exit(exitCode);
    }
} //end of SecondarySort
