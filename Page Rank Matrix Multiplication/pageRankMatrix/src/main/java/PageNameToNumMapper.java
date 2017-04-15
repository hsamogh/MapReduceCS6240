import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class PageNameToNumMapper extends Mapper <IntWritable, MapWritable, WritableComparable,Writable>{
	
	public HashMap<Integer, Long> offsetForPartition;
	public MapWritable pageNameToNumber;
	public MapWritable pageNumberToString;
	private MultipleOutputs outputs;

	public void setup(Context context) throws IOException{
		outputs = new MultipleOutputs(context);
		this.pageNameToNumber = new MapWritable();
		this.pageNumberToString = new MapWritable();
		this.offsetForPartition = new HashMap<Integer, Long>();
		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		URI[] paths = context.getCacheFiles();
		FileStatus[] files = fs.listStatus(new Path(paths[0].getPath()));
		this.createOffsetsForGivenSplits(files, conf);		
	}
	
	
	public void map(IntWritable key, MapWritable value, Context context){
		long offset = this.offsetForPartition.get(key.get());		
		for (Writable pageName : value.keySet() ){
			Text pageNameText = (Text)pageName;
			String pageNameString = pageNameText.toString();
			Text pageNameForHashMap = new Text(pageNameString);
			LongWritable relativePageNum = (LongWritable) value.get(pageName);
			LongWritable correctPageNum = new LongWritable (offset + relativePageNum.get());
			this.pageNameToNumber.put(pageNameForHashMap, correctPageNum);
			this.pageNumberToString.put(correctPageNum, pageNameForHashMap);
		}
	}
	

	
	public void cleanup(Context context) throws IOException, InterruptedException{
		outputs.write("dummy", NullWritable.get(), this.pageNameToNumber, CONSTANTS.StringToNumberFilePathPrefix);
		outputs.write(CONSTANTS.NumberToStringIdentifier, NullWritable.get(), this.pageNumberToString, CONSTANTS.NumberToStringPathPredix);
		outputs.close();
	}

	private void createOffsetsForGivenSplits(FileStatus[] files, Configuration conf) throws IOException{
		Long offset = 0L;
		for (int i = 0; i < files.length; i++) {
			Path currFile = files[i].getPath();
			if (currFile.toString()
					.contains(CONSTANTS.PrePrcoessOutputDirectory + CONSTANTS.OffsetSubDir)) {
				SequenceFile.Reader seqFileReader = new SequenceFile.Reader(conf, Reader.file(currFile));
				System.out.println("status: " + i + " " + currFile.toString());
				IntWritable key = new IntWritable();
				LongWritable value = new LongWritable();
				while (seqFileReader.next(key, value)) {
					this.offsetForPartition.put(key.get(), offset);
					offset+= value.get();
					System.out.println("Offset" + offset);
				}
				seqFileReader.close();
			}
		}
	}
}

