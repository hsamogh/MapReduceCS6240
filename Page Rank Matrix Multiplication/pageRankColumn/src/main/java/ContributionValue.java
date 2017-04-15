

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class ContributionValue implements Writable {
	public long row;
	public double contribution;
	public boolean isContributer;
	public boolean isPageRank;
	public boolean isDangling;
	
	public ContributionValue(){
		
	}
	public ContributionValue(long row, double contribution){
		this.row = row;
		this.contribution = contribution;
		this.isContributer = true;
		this.isPageRank = false;
		this.isDangling = false;
	}
	
	public ContributionValue(double pagerank){
		this.isContributer = false;
		this.contribution = pagerank;
		this.isPageRank = true;
		this.isDangling = false;
		this.row = -1;
	}
	
	public ContributionValue (long danglingNodeNumber){
		this.isContributer = false;
		this.contribution=0.0;
		this.isPageRank = false;
		this.isDangling = true;
		this.row = danglingNodeNumber;
	}
	

	public	void write(DataOutput out) throws IOException{
		out.writeLong(row);
		out.writeDouble(contribution);	
		out.writeBoolean(isContributer);
		out.writeBoolean(isPageRank);
		out.writeBoolean(isDangling);
		
	}
	  
	public void readFields(DataInput in) throws IOException{
	 	this.row = in.readLong();
	 	this.contribution = in.readDouble();
	 	this.isContributer = in.readBoolean();
	 	this.isPageRank = in.readBoolean();
	 	this.isDangling = in.readBoolean();
	}	
}
