import java.io.IOException;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class SMMultiMapper
		extends Mapper<LongWritable, ContributionValueWritable, LongWritable, ContributionValue> {
	
	public void map(LongWritable key, ContributionValueWritable value, Context context)
			throws IOException, InterruptedException {
		for (Writable val : value.get()) {
			ContributionValue colContributionval = (ContributionValue) val;
			context.write(new LongWritable(key.get()), new ContributionValue(colContributionval.row, colContributionval.contribution));
		}	
	}
}
