//This class is used to implement combiner
// Input : stationId and list of values of type WeatherRecord
// Output : Aggregated record of type WeatherCombiner

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

public class WeatherCombiner extends MapReduceBase implements Reducer<Text, WeatherRecord,Text,WeatherRecord> {


    public void reduce(Text text, Iterator<WeatherRecord> iterator, OutputCollector<Text, WeatherRecord> outputCollector,
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
        // emitting aggregated records
        outputCollector.collect(text,new WeatherRecord(new Text("TMAX"),new DoubleWritable(maxSum),new IntWritable(maxCount)));
        outputCollector.collect(text,new WeatherRecord(new Text("TMIN"),new DoubleWritable(minSum),new IntWritable(minCount)));



    }

}
