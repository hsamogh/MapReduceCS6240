import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<LongWritable, DoubleWritable, LongWritable, ContributionValue> {
	public void map(LongWritable key, DoubleWritable value, Context context)
			throws IOException, InterruptedException {
		context.write(key, new ContributionValue(value.get()));
	}
}
