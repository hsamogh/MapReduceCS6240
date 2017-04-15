import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class SMMultiMapper
		extends Mapper<LongWritable, ContributionValueWritable, NullWritable, MapWritable> {
	
	private long numberOfPages;
	private int iteration;
	private MapWritable pageRanks;
	private double danglingMass;
	private double previousDanglingMass;
	static double alpha = 0.15;
	static double oneMinusAlpha = 0.85;
	private MapWritable pageRankMap;

	
	double precision;
	
	
	public void setup(Context context) throws IOException {
		this.pageRankMap = new MapWritable();
		this.pageRanks = new MapWritable();
		Configuration conf = context.getConfiguration();
		this.numberOfPages = conf.getLong(CONSTANTS.NUMBEROFPAGES, 1);
		this.iteration = conf.getInt(CONSTANTS.IterationNumber, 0);
		this.previousDanglingMass = conf.getDouble(CONSTANTS.DanglingMass, 0);


		FileSystem fs = FileSystem.get(conf);
		URI[] paths = context.getCacheFiles();
		FileStatus[] files = fs.listStatus(new Path(paths[0].getPath()));		
		if (iteration > 0) {
			FileStatus[] pageRank = fs.listStatus(new Path(paths[1].getPath()));		

			this.formPageRankArray(pageRank, conf);
		}
		this.CalculateDanglingNode(files, conf, context);
	}

	public void formPageRankArray(FileStatus[] files, Configuration conf) throws IOException {
		for (int i = 0; i < files.length; i++) {
			Path currFile = files[i].getPath();
			if (currFile.toString()
					.contains(CONSTANTS.PrePrcoessOutputDirectory + (iteration - 1) + "/part")) {
				SequenceFile.Reader seqFileReader = new SequenceFile.Reader(conf, Reader.file(currFile));
				System.out.println("status: " + i + " " + currFile.toString());
				NullWritable key = NullWritable.get();
				MapWritable value = new MapWritable();
				while (seqFileReader.next(key, value)) {
					this.pageRanks.putAll(value);
				}
				seqFileReader.close();
			}
		}
	}

	public void CalculateDanglingNode(FileStatus[] files, Configuration conf, Context context) throws IOException {
		 this.danglingMass = 0.0;

		for (int i = 0; i < files.length; i++) {
			Path currFile = files[i].getPath();
			if (currFile.toString().contains(CONSTANTS.DanglingNodeSubDir)) {
				SequenceFile.Reader seqFileReader = new SequenceFile.Reader(conf, Reader.file(currFile));
				System.out.println("status: " + i + " " + currFile.toString());
				NullWritable key = NullWritable.get();
				LongWritable value = new LongWritable();
				while (seqFileReader.next(key, value)) {
					danglingMass += getRank(value.get()) * 1.0 / this.numberOfPages;
				}
				seqFileReader.close();
			}
		}
	}

	public void map(LongWritable key, ContributionValueWritable value, Context context)
			throws IOException, InterruptedException {
		double sum = 0.0;
		for (Writable val : value.get()) {
			ContributionValue colContributionval = (ContributionValue) val;
			
//			System.out.println("Contribution for Page : " +key.get() + "from"  + colContributionval.col + "is " + colContributionval.contribution );

			sum += getRank(colContributionval.col) * colContributionval.contribution;
		}
		double pageRank = alpha/numberOfPages + oneMinusAlpha * (danglingMass + sum);
		this.precision += pageRank;
		pageRankMap.put(new LongWritable(key.get()), new DoubleWritable(pageRank));
		
	}

	

	public double getRank(long pageNumber) {
		if (this.iteration == 0) {
			return 1.0 / this.numberOfPages;
		}
		
		LongWritable key = new LongWritable(pageNumber);
		if(this.pageRanks.containsKey(key)){
			DoubleWritable pageRank = (DoubleWritable) this.pageRanks.get(key);
			return pageRank.get();
		}
		else{
			return (alpha/numberOfPages + oneMinusAlpha * (previousDanglingMass));
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		int mapId = context.getTaskAttemptID().getTaskID().getId();
		if(mapId == 0){
			context.getCounter(PageRankDriver.counter.delta).setValue(Double.doubleToLongBits(this.danglingMass));
		}
		System.out.println(this.precision);
		context.write(NullWritable.get(), this.pageRankMap);
	}

}
