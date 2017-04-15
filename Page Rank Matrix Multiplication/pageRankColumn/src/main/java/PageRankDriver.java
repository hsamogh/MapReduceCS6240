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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
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
	

	//Counter Enums
	static enum counter{
		NumberOfPages,
		delta
	}
	
	//Driver Program
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
          System.exit(2);
        }
		
//      Setup PreProcessing Job Step 1 and Step 2

		
		Job preprocessJob =  getPreProcessJob(otherArgs, conf);
		preprocessJob.waitForCompletion(true);
		
		long numberOfPages = preprocessJob.getCounters().findCounter(counter.NumberOfPages).getValue();		
        conf.setLong(CONSTANTS.NUMBER_OF_PAGES, numberOfPages);

		
		Job pageNameToNum = createPreProcessStringToNumberMappingJob(conf, numberOfPages);		
		pageNameToNum.waitForCompletion(true);	
		
		Job sparseMatrix = createSparseMatrix(conf);
		sparseMatrix.waitForCompletion(true);


		double delta = 0.0;

  
//      PageRank Computation
		for (int i = 0; i < 10; i++){
			conf.setInt(CONSTANTS.ITER_NUMBER, i);
			Job multiJob =  getMatrixMulJob(conf, i);
			multiJob.waitForCompletion(true);
			Job matrixAggregateJob =  getMatrixAggregateJob(conf, i);
			matrixAggregateJob.waitForCompletion(true);
			delta = Double.longBitsToDouble(matrixAggregateJob.getCounters().findCounter(counter.delta).getValue());
			conf.setDouble(CONSTANTS.DANGLING_WEIGHT, delta);

		}
		
//		Top 100 Page Ranked Pages
		Job topKPages = getTopKJob(otherArgs[otherArgs.length - 1], conf);
		topKPages.waitForCompletion(true);

		

		System.exit(0);
  }
	
	//Computes the ADJ List and local worker machine Page numbers
	public static Job getPreProcessJob(String [] args, Configuration conf) throws IOException{
		Job job = new Job(conf, "Pre Process");
        job.setJarByClass(PageRankDriver.class);
        job.setReducerClass(ParseReducer.class);
        job.setMapperClass(ParseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);

        MultipleOutputs.addNamedOutput(job,CONSTANTS.NumOffsetFileIdentifier,
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
	
	//Creates Global Unique Numbers for all Pages
	public static Job createPreProcessStringToNumberMappingJob (Configuration conf, long numOfPages) throws IllegalArgumentException, IOException, URISyntaxException{		
		Job job = new Job(conf, "NAME2NUMBER");
		

		job.addCacheFile(new Path(CONSTANTS.PrePrcoessOutputDirectory +  CONSTANTS.OffsetSubDir).toUri());
		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(NameToNumMapper.class);
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
	
	// Creates the matrix using Global Number to PageName Mapping and Adj List
	public static Job createSparseMatrix(Configuration conf) throws IOException, URISyntaxException{		
		Job job = new Job(conf, "SPARSE_MATRIX");
//
		job.addCacheFile(new Path(CONSTANTS.PageNameToNumberOutputDirectory + CONSTANTS.stringToMapInput).toUri());
        job.setJarByClass(PageRankDriver.class);
        job.setReducerClass(SparseMatrixReducer.class);
        job.setMapperClass(SparseMatrixMapper.class);
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
	
	public static Job getMatrixMulJob(Configuration conf, int iteration) throws IOException, URISyntaxException{
		Job job = new Job(conf, "Matrix Mul");
		job.setJarByClass(PageRankDriver.class);
		MultipleInputs.addInputPath(job, new Path(CONSTANTS.SpareMatrixOutputDir + CONSTANTS.MatrixSubDir), SequenceFileInputFormat.class, SMMultiMapper.class);
		MultipleInputs.addInputPath(job, new Path(CONSTANTS.SpareMatrixOutputDir + CONSTANTS.DanglingNodeSubDir), SequenceFileInputFormat.class, DanglingNodeMapper.class);
		if(iteration > 0){
			MultipleInputs.addInputPath(job, new Path(CONSTANTS.AggrOutputPath + (iteration - 1)), SequenceFileInputFormat.class, PageRankMapper.class);
		}
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(ContributionValue.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setReducerClass(SMMultiReducer.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		MultipleOutputs.addNamedOutput(job, CONSTANTS.colContributionFileIdenfier, SequenceFileOutputFormat.class,
       		 LongWritable.class, DoubleWritable.class);		
		MultipleOutputs.addNamedOutput(job, CONSTANTS.danglingMassFileIdentifier,
				 SequenceFileOutputFormat.class,
		   NullWritable.class, DoubleWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(CONSTANTS.PrePrcoessOutputDirectory + iteration));
		return job;
	}
	
	public static Job getMatrixAggregateJob(Configuration conf, int iteration) throws IOException, URISyntaxException{
		Job job = new Job(conf, "Matrix Aggr");

		job.addCacheFile(new Path(CONSTANTS.PrePrcoessOutputDirectory + iteration + CONSTANTS.danglingMassSubDir).toUri());

		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(SMAggregateMapper.class);
		job.setReducerClass(SMAggregateReducer.class);
		job.setCombinerClass(SpaseMatrixAggrCombiner.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);		
		FileInputFormat.addInputPath(job, new Path(CONSTANTS.PrePrcoessOutputDirectory + iteration + CONSTANTS.colContributionSubDir));
		FileOutputFormat.setOutputPath(job, new Path(CONSTANTS.AggrOutputPath + iteration));
		return job;
	}
	
	public static Job getTopKJob(String outputPath,  Configuration conf) throws IOException, URISyntaxException{
		Job job = new Job(conf, "topK");
		job.addCacheFile(new Path(CONSTANTS.PageNameToNumberOutputDirectory + CONSTANTS.NumberToStringDir).toUri());
		job.setJarByClass(PageRankDriver.class);
		job.setMapperClass(TopKMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(TopKReducer.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		FileInputFormat.addInputPath(job, new Path(CONSTANTS.AggrOutputPath + "9"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		return job;
	}
}
