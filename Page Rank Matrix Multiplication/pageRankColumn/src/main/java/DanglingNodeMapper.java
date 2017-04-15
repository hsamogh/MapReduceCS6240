import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class DanglingNodeMapper extends Mapper<NullWritable, LongWritable, LongWritable, ContributionValue> {
	public void map(NullWritable key, LongWritable value, Context context)
			throws IOException, InterruptedException {
		context.write(value, new ContributionValue(value.get()));
	}
}