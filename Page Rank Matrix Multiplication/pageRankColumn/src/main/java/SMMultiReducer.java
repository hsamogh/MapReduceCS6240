import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.commons.collections.iterators.EntrySetMapIterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class SMMultiReducer extends Reducer<LongWritable, ContributionValue, LongWritable, DoubleWritable> {

	private long numberOfPages;
	private int iteration;
	private HashMap<Long, Double> rowContribution;
	private ArrayList<ContributionValue> pageContributions;
	private double localDanglingMass;
	private double previousDanglingMass;
	private MultipleOutputs mos;


	static double alpha = 0.15;
	static double oneMinusAlpha = 1 - alpha;
	
	public void setup(Context context){		
		Configuration conf = context.getConfiguration();
		this.mos = new MultipleOutputs(context);
		this.rowContribution = new HashMap<Long, Double>();
		this.pageContributions = new ArrayList<ContributionValue>();
		this.localDanglingMass = 0.0;	
		this.numberOfPages = conf.getLong(CONSTANTS.NUMBER_OF_PAGES, 1);
		this.iteration = conf.getInt(CONSTANTS.ITER_NUMBER, 0);
		this.previousDanglingMass = conf.getDouble(CONSTANTS.DANGLING_WEIGHT, 0);
	}
	
	public void reduce(LongWritable key, Iterable<ContributionValue> values, Context context)
	{
		boolean contributeToDangling = false;
		double pageRank = 0.0;
		for(ContributionValue value: values){
			if(value.isContributer){
				this.pageContributions.add(new ContributionValue(value.row, value.contribution));
			}
			else if(value.isPageRank){
				pageRank = value.contribution;
			}
			else{
				contributeToDangling = true;
			}			
		}
		// Missing Ranks for pages which do not have contributions
		if(pageRank == 0.0){
			pageRank = alpha/numberOfPages + oneMinusAlpha*(previousDanglingMass);
		}
		
		// For 1st iterations default page rank 1/n
		if(iteration == 0){
			pageRank = 1.0/numberOfPages;
		}
		this.updateContributions(pageRank,  contributeToDangling);
	}
	
	private void updateContributions(double pageRank, boolean contributeToDangling){
		if(contributeToDangling){
			this.localDanglingMass += pageRank/numberOfPages;
		}
		for(ContributionValue value : this.pageContributions){
			this.addToRowContribution(value.row, pageRank*value.contribution);
		}
		this.resetContributions();
	}
	
	private void resetContributions(){
		this.pageContributions.clear();		
	}
	
	private void addToRowContribution(long row, double contri){
		if(this.rowContribution.containsKey(row)){
			double accumulatedContri = this.rowContribution.get(row);
			accumulatedContri += contri;
			this.rowContribution.put(row, accumulatedContri);
		}
		else{
			this.rowContribution.put(row, contri);
		}
	}
	
	private void outputToDisk(Context ctx) throws IOException, InterruptedException{
		for(Long key : this.rowContribution.keySet()){
			mos.write(CONSTANTS.colContributionFileIdenfier, new LongWritable(key), new DoubleWritable(this.rowContribution.get(key)), CONSTANTS.colContributionPathPrefic);
		}
	}
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		this.outputToDisk(context);
		mos.write(CONSTANTS.danglingMassFileIdentifier, NullWritable.get(), new DoubleWritable(this.localDanglingMass), CONSTANTS.danglingMassPathPrefic);
		mos.close();
	}
}
