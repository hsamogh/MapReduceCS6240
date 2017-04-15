import java.io.IOException;
import java.net.URI;

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
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SRMMapper extends Mapper <Text, Node, WritableComparable, Writable>{
	
	private MapWritable pageNameToNumberMap;
	
	private MultipleOutputs outputs;
	
	public void setup(Context context) throws IOException{
		
		this.pageNameToNumberMap = new MapWritable();
		outputs = new MultipleOutputs(context);

		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		URI[] paths = context.getCacheFiles();
		FileStatus[] files = fs.listStatus(new Path(paths[0].getPath()));
		this.setupPageNameToNumberMap(files, conf);	
	}
	
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		outputs.close();
	}
	
	public void map(Text key, Node value, Context ctx) throws IOException, InterruptedException{
		LongWritable keyToLongWritbale = (LongWritable)this.pageNameToNumberMap.get(key);
		for(String node: value.adjacencyList){
			LongWritable nodeToLongWritbale = (LongWritable)this.pageNameToNumberMap.get(new Text(node));
			ctx.write(nodeToLongWritbale , new ContributionValue(keyToLongWritbale.get(), 1.0/value.adjacencyList.size()));
		}
		if(!value.hasAdjacencyList || value.adjacencyList.size() == 0){
			outputs.write(CONSTANTS.DanglingNodeFileIdentifier, NullWritable.get(), keyToLongWritbale, CONSTANTS.DanglingPathPrefix);
		}
	}
	
	private void setupPageNameToNumberMap(FileStatus[] files, Configuration conf) throws IOException{
		for (int i = 0; i < files.length; i++) {
			Path currFile = files[i].getPath();
			if (currFile.toString()
					.contains(CONSTANTS.PageNameToNumberOutputDirectory + CONSTANTS.stringToMapInput)) {
				SequenceFile.Reader seqFileReader = new SequenceFile.Reader(conf, Reader.file(currFile));
				NullWritable key = NullWritable.get();
				MapWritable value = new MapWritable();
				while (seqFileReader.next(key, value)) {
					this.pageNameToNumberMap.putAll(value);					
				}
				seqFileReader.close();
			}
		}
	}
}
