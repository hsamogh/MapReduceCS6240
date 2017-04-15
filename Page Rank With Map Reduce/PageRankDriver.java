/**
 * Created by amogh on 2/20/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class PageRankDriver extends Configured implements Tool {


    public static String iterationFlag = "iterationFlag";
    public static String pageCount = "pageCount";
    public static String delta = "deltaSum";

    //setting up global hadoop counters
    public static enum globalCounter {
        pageCount,
        delta
    }

    //function to run map reduce jobs
    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] newArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        long startTime;
        long endTime;
        long execTime;



        // logic to handle incorrect number of commandline arguments
        if (newArgs.length < 2) {
            System.out.println("Invalid number of command line arguments.");
            return 1;
        }

        // Map reduce job to pre-process data for computing page rank
        startTime = System.currentTimeMillis();
        Job preprocessingJob = preProcessingJob(newArgs, conf);
        preprocessingJob.waitForCompletion(true);
        endTime = System.currentTimeMillis();
        execTime = endTime-startTime;
        System.out.println("Time taken for preProcessing is " + execTime);
        long pageCount = preprocessingJob.getCounters().findCounter(globalCounter.pageCount).getValue();

        //initialising variables
        double delta = 0;
        int count = 0;

        //Map reduce job to compute page Rank

        startTime = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            count++;
            Job pageRankJob = pageRankJob(args, conf, pageCount, i, delta);
            pageRankJob.waitForCompletion(true);
            delta = Double.longBitsToDouble(pageRankJob.getCounters().findCounter(globalCounter.delta).getValue());
        }
        endTime = System.currentTimeMillis();
        execTime = endTime-startTime;
        System.out.println("Time taken for 10 iterations of Page Rank  is " + execTime);

        //Map reduce job to find top 100 records with highest page ranks
        startTime = System.currentTimeMillis();
        Job topKPages = TopKRanksJob("processed_data" + count, newArgs[newArgs.length - 1], conf);
        topKPages.waitForCompletion(true);
        endTime = System.currentTimeMillis();
        execTime = endTime-startTime;
        System.out.println("Time taken to find top 100 pages is " + execTime);

        return 0;
    }

    // Function to create map reduce job to get Top-100 Records
    public static Job TopKRanksJob(String inputPath, String outputPath, Configuration conf) throws IOException {

        Job j = new Job(conf, "TopKRanks");
        j.setJarByClass(PageRankDriver.class);
        j.setMapperClass(TopKMapper.class);
        j.setOutputKeyClass(NullWritable.class);
        j.setOutputValueClass(Text.class);
        j.setReducerClass(TopKReducer.class);
        j.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(j, new Path(inputPath));
        FileOutputFormat.setOutputPath(j, new Path(outputPath));
        return j;
    }

    // Function to create map reduce job to preprocess data set
    public static Job preProcessingJob(String[] args, Configuration conf) throws IOException {

        Job j1 = new Job(conf, "PreProcessing");
        j1.setJarByClass(PageRankDriver.class);
        j1.setReducerClass(PreProcessReducer.class);
        j1.setMapperClass(PreProcessMapper.class);
        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(Node.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Node.class);
        j1.setOutputFormatClass(SequenceFileOutputFormat.class);
        for (int i = 0; i < args.length - 1; ++i) {
            FileInputFormat.addInputPath(j1, new Path(args[i]));
        }
        FileOutputFormat.setOutputPath(j1, new Path("processed_data0"));
        return j1;

    }


    // Function to create map reduce job to compute page rank
    public static Job pageRankJob(String[] args, Configuration conf, long pageCount, int iterationCount, double delta) throws IOException {

        conf.setLong("pageCount", pageCount);
        conf.setDouble("delta", delta);
        if (iterationCount == 0) {
            conf.setBoolean(iterationFlag, true);
        } else {
            conf.setBoolean(iterationFlag, false);
        }
        Job job = new Job(conf, "PageRank");
        job.setJarByClass(PageRankDriver.class);
        job.setReducerClass(PageRankReducer.class);
        job.setCombinerClass(PageRankCombiner.class);
        job.setMapperClass(PageRankMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("processed_data" + iterationCount));
        FileOutputFormat.setOutputPath(job,
                new Path("processed_data" + (iterationCount + 1)));
        return job;


    }

    //Main function : the entry point for program
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new PageRankDriver(), args);
        System.exit(exitCode);
    }
}
