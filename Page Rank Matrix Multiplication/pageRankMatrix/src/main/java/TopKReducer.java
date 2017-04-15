import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.mapreduce.Reducer;

// Reducer Class
public class TopKReducer extends Reducer<LongWritable, DoubleWritable, NullWritable, Text> {
	
	
		
		private TreeMap<DoubleWritable, LongWritable> topK;
		
		private MapWritable numToPageNameMap;
		
		public void setup(Context context) throws IOException{
			this.topK = new TreeMap<DoubleWritable, LongWritable>();

			this.numToPageNameMap = new MapWritable();
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			URI[] paths = context.getCacheFiles();
			FileStatus[] files = fs.listStatus(new Path(paths[0].getPath()));
			this.formNumToPageMapers(files, conf);
		}
		
		public void reduce(LongWritable key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {	
			
			for (DoubleWritable value : values) {

				topK.put(new DoubleWritable(value.get()), new LongWritable(key.get()));
				if (topK.size() > 100) {
					topK.remove(topK.firstKey());
				}
			}			
			// Output the content of treemap			
		}
		
		public void cleanup(Context context) throws IOException, InterruptedException{
			NavigableMap<DoubleWritable, LongWritable> reverseMap = topK.descendingMap();
			for (Map.Entry<DoubleWritable, LongWritable> t : reverseMap.entrySet()) {
				context.write(NullWritable.get(), new Text(t.getKey().toString() +" " + this.numToPageNameMap.get(t.getValue()).toString()));
			}
		}
		
		public void formNumToPageMapers(FileStatus[] files, Configuration conf) throws IOException {
			for (int i = 0; i < files.length; i++) {
				Path currFile = files[i].getPath();
				if (currFile.toString()
						.contains(CONSTANTS.PageNameToNumberOutputDirectory + CONSTANTS.NumberToStringDir)) {
					SequenceFile.Reader seqFileReader = new SequenceFile.Reader(conf, Reader.file(currFile));
					System.out.println("status: " + i + " " + currFile.toString());
					NullWritable key = NullWritable.get();
					MapWritable value = new MapWritable();
					double danglingMass = 0.0;
					while (seqFileReader.next(key, value)) {
						this.numToPageNameMap.putAll(value);
					}
					seqFileReader.close();
				}
			}
		}
	}
