import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SpaseMatrixAggrCombiner extends Reducer<LongWritable, DoubleWritable, LongWritable, DoubleWritable> { 
    
	public void combine(LongWritable key, Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException{
        double sum = 0; 
        for (DoubleWritable val : values) {
           sum += val.get();
        }
        context.write(key, new DoubleWritable(sum));
    }
}