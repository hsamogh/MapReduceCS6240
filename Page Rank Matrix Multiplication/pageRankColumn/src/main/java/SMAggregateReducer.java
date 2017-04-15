import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Reducer;

public class SMAggregateReducer extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> {
	
	static double alpha = 0.15;
	static double oneMinusAlpha = 1 - alpha;
	private long numberOfPages;
	private double danglingMass;	
	
	public void setup(Context ctx) throws IOException{
		Configuration conf = ctx.getConfiguration();
		this.numberOfPages = conf.getLong(CONSTANTS.NUMBER_OF_PAGES, 1);
		this.danglingMass = 0.0;
		
		FileSystem fs = FileSystem.get(conf);
		URI[] paths = ctx.getCacheFiles();
		FileStatus[] files = fs.listStatus(new Path(paths[0].getPath()));
		this.CalculateDanglingMass(files, conf, ctx);
	}
	
	public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context ctx) throws IOException, InterruptedException{
		double sum = 0.0;
		for (DoubleWritable val : values) {
           sum += val.get();
        }
		double pageRank = alpha/this.numberOfPages + oneMinusAlpha * (this.danglingMass + sum);
		ctx.write(new LongWritable(key.get()), new DoubleWritable(pageRank));
	}
	
	public void cleanup(Context ctx) throws IOException, InterruptedException{
		int reduceId = ctx.getTaskAttemptID().getTaskID().getId();		
		if(reduceId == 0){
			ctx.getCounter(PageRankDriver.counter.delta).setValue(Double.doubleToLongBits(this.danglingMass));
		}
	}
	
	private void CalculateDanglingMass(FileStatus[] files, Configuration conf, Context context) throws IOException {
		 this.danglingMass = 0.0;

		for (int i = 0; i < files.length; i++) {
			Path currFile = files[i].getPath();
			if (currFile.toString().contains(CONSTANTS.danglingMassSubDir)) {
				Reader seqFileReader = new Reader(conf, Reader.file(currFile));
				NullWritable key = NullWritable.get();
				DoubleWritable value = new DoubleWritable();
				while (seqFileReader.next(key, value)) {
					this.danglingMass += value.get();
				}
				seqFileReader.close();
			}
		}
	}	
}
