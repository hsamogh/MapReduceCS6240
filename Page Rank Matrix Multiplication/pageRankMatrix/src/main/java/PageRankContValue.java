import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

public class PageRankContValue implements Writable{
	
	public double contribution;// stores actual page rank or has contribution

	public PageRankContValue(){
		this.contribution = 0.0;
				
	}
	
	public PageRankContValue(double pageRank, boolean isContributer){
		this.contribution = pageRank;
	}
	

	public	void write(DataOutput out) throws IOException{
		out.writeDouble(contribution);
	}
	  
	 public void readFields(DataInput in) throws IOException{
		 	this.contribution = in.readDouble();
	 }
}
