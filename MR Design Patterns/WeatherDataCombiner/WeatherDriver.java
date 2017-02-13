/***
 *
 *  This program is part of the assignment used to compare different design patterns in map reduce. We use combiner
 *  approach in this code
 *
 *  This program calculates mean maximum and mean minimum of temperatures recorded by various stations. the mean is
 *  calculated per station-id . we are calculating means for the temperatures for the year 1991
 *
 *  Inputs :
 *
 *  input-file-path : absolute path of the file containing readings
 *  output-file-path : path where the output needs to be stored. The output folder should not be present at the path
 *                     specified. Map reduce code creates the output folder.
 *
 */


//import statements
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// The driver class
public class WeatherDriver extends Configured implements Tool{

    // methods from Tool interface
    //Input : list of arguments specified via command line parameters
    public int run(String[] args) throws Exception {


        // Checking if all command line arguments are supplied
        if(args.length < 2){
            System.out.println("One or more command line parameters missing. Check readMe.txt for more info");
            return -1;
        }

        //Creating jobConf object
        JobConf conf = new JobConf(WeatherDriver.class);

        //setting file input and output parameters
        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        //specifying mapper, combiner and reducer class
        conf.setMapperClass(WeatherMapper.class);
        conf.setReducerClass(WeatherReducer.class);
        conf.setCombinerClass(WeatherCombiner.class);

        //setting data types of mapper outputs
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(WeatherRecord.class);

        //running the job
        JobClient.runJob(conf);

        return 0;
    }


    // Entry point of the program
    public static void main(String[] args)throws Exception
    {
        int exitCode = ToolRunner.run(new WeatherDriver(),args);

        System.exit(exitCode);
    }

}
