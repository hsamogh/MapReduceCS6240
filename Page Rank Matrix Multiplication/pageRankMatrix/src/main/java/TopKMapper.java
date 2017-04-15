import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class TopKMapper extends Mapper <NullWritable, MapWritable, LongWritable, DoubleWritable> {
	private TreeMap<DoubleWritable, LongWritable> topK;
	
	public void setup(Context con){
		this.topK = new TreeMap<DoubleWritable, LongWritable>();
	}
	

	@Override
	public void map(NullWritable key, MapWritable value, Context context)
			throws IOException, InterruptedException {
		
		for (Map.Entry<Writable, Writable> entry : value.entrySet()) {
			topK.put((DoubleWritable)entry.getValue(),(LongWritable)entry.getKey());
			if (topK.size() > 100) {
				topK.remove(topK.firstKey());
			}
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Map.Entry<DoubleWritable, LongWritable> entry : topK.entrySet()) {
			context.write(entry.getValue(), entry.getKey());

		}
	}
}
