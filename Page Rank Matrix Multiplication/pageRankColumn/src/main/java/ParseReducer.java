import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class ParseReducer extends Reducer <Text, Node, WritableComparable, Writable> {
	
	private MultipleOutputs outputs;

	private long counter;
	
	private MapWritable rowColNumberPageNameMap;
	
	private ArrayList<String> danglingNodes;		
	
	public void setup(Context context){
		outputs = new MultipleOutputs(context);
		this.counter = 0;
		this.rowColNumberPageNameMap  = new MapWritable();
		this.danglingNodes = new ArrayList<String>();
	}
	
	public void reduce(Text key, Iterable<Node> value, Context context) throws IOException, InterruptedException{
		boolean hasAdjList = false;		
		context.getCounter(PageRankDriver.counter.NumberOfPages).increment(1);
		this.addToMap(key.toString());
		Node node = new Node();
		for(Node nodeValue: value){
			if(nodeValue.hasAdjacencyList && nodeValue.adjacencyList.size() > 0){
				node.adjacencyList.addAll(nodeValue.adjacencyList);
				node.hasAdjacencyList = true;
				node.adjacencyList.addAll(nodeValue.adjacencyList);
		}
		}
		outputs.write(CONSTANTS.AdjacencyListFileIdentifier, new Text(key.toString()), node, CONSTANTS.AdjacencyListFilePathPrefix);

	}
	
	
	
	public void cleanup(Context context) throws IOException, InterruptedException{
		int reduceId = context.getTaskAttemptID().getTaskID().getId();
		outputs.write(CONSTANTS.StringToNumberMapFileIdentifier,new IntWritable(reduceId), this.rowColNumberPageNameMap, CONSTANTS.StringToNumberFilePathPrefix);
		outputs.write(CONSTANTS.NumOffsetFileIdentifier, new IntWritable(reduceId), new LongWritable(this.counter), CONSTANTS.OffsetFilePathPrefix);
		outputs.close();
	}
	
	
	public void addToMap(String name){
		Text pageName = new Text(name);
		if(!this.rowColNumberPageNameMap.containsKey(pageName)){
			LongWritable rowNumber = new LongWritable(this.counter++);
			this.rowColNumberPageNameMap.put(pageName, rowNumber);
		}
	}	
}

