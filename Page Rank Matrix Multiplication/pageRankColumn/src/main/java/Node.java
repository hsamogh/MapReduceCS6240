import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Writable;

public class Node implements Writable{
	public boolean hasAdjacencyList; // to check if it a dangling node or not
	public ArrayList<String> adjacencyList;

	public Node(){
		this.hasAdjacencyList = false;
		this.adjacencyList = new ArrayList<String>();
	}
	

	public	void write(DataOutput out) throws IOException{
		out.writeBoolean(hasAdjacencyList);
		out.writeInt(adjacencyList.size());
		for (String node : adjacencyList){
			out.writeUTF(node);
		}
	}	  
	
	public void readFields(DataInput in) throws IOException{
	 	this.hasAdjacencyList = in.readBoolean();
	 	this.adjacencyList = new ArrayList<String>();
	 	int arrayLength = in.readInt();
	 	for(int i = 0; i< arrayLength; i++){
	 		this.adjacencyList.add(in.readUTF());
	 	}
	}
}
