import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

// Main Class Driver Program
public class PageRankDriver{


	static enum counter{
		NumberOfPages,
		delta
	}
	

	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2)
          System.exit(2);

		Job preprocessJob =  createPreProcessJob(otherArgs, conf);
		preprocessJob.waitForCompletion(true);
		long numberOfPages = preprocessJob.getCounters().findCounter(counter.NumberOfPages).getValue();		
        conf.setLong(CONSTANTS.NUMBEROFPAGES, numberOfPages);
		
		Job pageNameToNum = getPreProcessStringToNumberMappingJob(conf, numberOfPages);
		pageNameToNum.waitForCompletion(true);	
		
		Job sparseMatrix = getSparseMatrixJob(conf);
		sparseMatrix.waitForCompletion(true);


		double delta = 0.0;  

		for (int i = 0; i < 10; i++){
			long pageRankStartTime = System.currentTimeMillis();
			conf.setInt(CONSTANTS.IterationNumber, i);
			Job matrixmulJob =  getMatrixMultiJob(conf, i);
			matrixmulJob.waitForCompletion(true);
			delta = Double.longBitsToDouble(matrixmulJob.getCounters().findCounter(counter.delta).getValue());
			conf.setDouble(CONSTANTS.DanglingMass, delta);
			long pageRankendEndTime = System.currentTimeMillis();
			long pageRankexecTime = pageRankendEndTime - pageRankStartTime;
			System.out.println("Step 3 : PageRank For Iteration "+ i + " : " + pageRankexecTime + " ms");
		}		

		long topKRankStartTime = System.currentTimeMillis();
		Job topKPages = getTopKJob(otherArgs[otherArgs.length - 1], conf);
		topKPages.waitForCompletion(true);

  }


	public static Job getTopKJob(String outputPath,  Configuration conf) throws IOException, URISyntaxException{

		Job job = new Job(conf, "TOP_K");
		job.addCacheFile(new Path(CONSTANTS.PageNameToNumberOutputDirectory +  CONSTANTS.NumberToStringDir).toUri());
		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(TopKMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(TopKReducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path("output9"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}
	

	
	public static Job createPreProcessJob(String [] args, Configuration conf) throws IOException{        
		Job job = new Job(conf, "Pre Process");
        job.setJarByClass(PageRankDriver.class);
        job.setReducerClass(ParseReducer.class);
        job.setMapperClass(ParseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);

        MultipleOutputs.addNamedOutput(job, CONSTANTS.NumOffsetFileIdentifier,
				 SequenceFileOutputFormat.class,
		   IntWritable.class, LongWritable.class);
        
        MultipleOutputs.addNamedOutput(job, CONSTANTS.AdjacencyListFileIdentifier,
				 SequenceFileOutputFormat.class,
		   Text.class, Node.class);
		
		MultipleOutputs.addNamedOutput(job, CONSTANTS.StringToNumberMapFileIdentifier,
				 SequenceFileOutputFormat.class,
		   IntWritable.class, MapWritable.class);		
		
        for (int i = 0; i < args.length - 1; ++i) {
          FileInputFormat.addInputPath(job, new Path(args[i]));
        }
        
        FileOutputFormat.setOutputPath(job,
          new Path("output"));
        return job;
	}

	public static Job getSparseMatrixJob(Configuration conf) throws IOException, URISyntaxException{

		Job job = new Job(conf, "Sparse Matrix");

		job.addCacheFile(new Path(CONSTANTS.PageNameToNumberOutputDirectory  +  CONSTANTS.stringToMapInput).toUri());

		job.setJarByClass(PageRankDriver.class);
		job.setReducerClass(SRMReducer.class);
		job.setMapperClass(SRMMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(ContributionValue.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(ContributionValueWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		MultipleOutputs.addNamedOutput(job, CONSTANTS.DanglingNodeFileIdentifier, SequenceFileOutputFormat.class,
				NullWritable.class, LongWritable.class);

		MultipleOutputs.addNamedOutput(job, CONSTANTS.MatrixFileIdentifier,
				SequenceFileOutputFormat.class,
				LongWritable.class, ContributionValueWritable.class);
		FileInputFormat.addInputPath(job, new Path(CONSTANTS.PrePrcoessOutputDirectory + CONSTANTS.AdjacenceyListSubDirectory));
		FileOutputFormat.setOutputPath(job, new Path(CONSTANTS.SpareMatrixOutputDir));
		return job;
	}



	public static Job getMatrixMultiJob(Configuration conf, int iteration) throws IOException, URISyntaxException{
		Job job = new Job(conf, "Multiplication");

		job.addCacheFile(new Path(CONSTANTS.SpareMatrixOutputDir +  "/dangling").toUri());

		if(iteration > 0){

			job.addCacheFile(new Path( CONSTANTS.PrePrcoessOutputDirectory + (iteration-1)).toUri());
		}
		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(SMMultiMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(MapWritable.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(CONSTANTS.SpareMatrixOutputDir + CONSTANTS.MatrixSubDir));
		FileOutputFormat.setOutputPath(job, new Path(CONSTANTS.PrePrcoessOutputDirectory + iteration));
		return job;
	}
	
	public static Job getPreProcessStringToNumberMappingJob (Configuration conf, long numOfPages) throws IllegalArgumentException, IOException, URISyntaxException{
		
		Job job = new Job(conf, "PageName to Number Job");
		job.addCacheFile(new Path(CONSTANTS.PrePrcoessOutputDirectory +  CONSTANTS.OffsetSubDir).toUri());
		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(PageNameToNumMapper.class);
		job.setOutputKeyClass(WritableComparable.class);
		job.setOutputValueClass(Writable.class);
		job.setNumReduceTasks(0);// No reducer
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(CONSTANTS.PrePrcoessOutputDirectory + CONSTANTS.stringToMapInput));
		FileOutputFormat.setOutputPath(job, new Path(CONSTANTS.PageNameToNumberOutputDirectory));
		
		MultipleOutputs.addNamedOutput(job, CONSTANTS.NumberToStringIdentifier,
				SequenceFileOutputFormat.class,
				   NullWritable.class, MapWritable.class);
		
		MultipleOutputs.addNamedOutput(job, "dummy",
				 SequenceFileOutputFormat.class,
		   NullWritable.class, MapWritable.class);
		
		return job;
	}
	
	

	

	

}
