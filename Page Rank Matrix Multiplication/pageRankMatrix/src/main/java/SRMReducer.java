import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SRMReducer extends Reducer<LongWritable,ContributionValue, LongWritable, ContributionValueWritable>{

	private MultipleOutputs outputs;

	public void setup(Context context){
		outputs = new MultipleOutputs(context);

	}
	
	public void reduce(LongWritable key, Iterable<ContributionValue> value, Context context) throws IOException, InterruptedException{
		ArrayList<ContributionValue> arraylist = new ArrayList<ContributionValue>();
		for(ContributionValue val : value){
			arraylist.add(new ContributionValue(val.col, val.contribution));
		}
		ContributionValue [] list = new ContributionValue[arraylist.size()];
		int i = 0;
		for(ContributionValue val :arraylist ){
			list[i++] = val;
		}
		ContributionValueWritable arrayWritableList = new ContributionValueWritable();
		arrayWritableList.set(list);
		outputs.write(CONSTANTS.MatrixFileIdentifier, new LongWritable(key.get()), arrayWritableList, CONSTANTS.MatrixPathPrefix);
	}

	public void cleanup(Context context) throws IOException, InterruptedException{
		outputs.close();
	}
	
	
	
}

