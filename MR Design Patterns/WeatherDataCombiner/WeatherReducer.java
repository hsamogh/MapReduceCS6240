/**
 *
 *  This class implements reducer functionality . Implements reducer interface
 *   Input Key : station-id
 *   Input Value :  WeatherRecord Object represented by WeatherRecord Class
 *   Output Key : station-id
 *   Output Value : Text containing mean minimum temperature and mean maximum temperature
 *
 */


//import  statements

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;


//reducer class . implements reducer interface
public class WeatherReducer extends MapReduceBase implements Reducer <Text, WeatherRecord,Text,Text> {

    // implementing the unimplemented methods in reducer interface
    public void reduce(Text key, Iterator<WeatherRecord> iterator, OutputCollector<Text, Text> outputCollector,
                       Reporter reporter) throws IOException {

        // initializing local variables to compute average
        int  maxCount =0;
        int minCount=0;
        double maxSum=0;
        double minSum=0;

        //iterating over list of values to compute average
        while(iterator.hasNext()){
            WeatherRecord record = iterator.next();
            if(record.type.toString().equals("TMAX")){
                maxSum += record.getReading();
                maxCount += record.getCount();
            }
            else if(record.type.toString().equals("TMIN")){
                minSum += record.getReading();
                minCount+=record.getCount();
            }
        }

        String maxAvg,minAvg = "";


        // logic to handle divide by zero case

        if(minCount==0){
            minAvg= "None";
        }else{
          minAvg = Double.toString(minSum/minCount);
        }
        if(maxCount>0){
            maxAvg=Double.toString(maxSum/maxCount);
        }else{
          maxAvg = "None";
        }

        outputCollector.collect(key, new Text(","+minAvg+","+maxAvg));



    }

}
