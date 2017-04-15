/**
 * Created by amogh-hadoop on 2/21/17.
 */

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.ArrayList;

public class PreProcessReducer extends Reducer<Text, Node, Text, Node> {

    public void reduce(Text key, Iterable<Node> value, Context context) throws IOException, InterruptedException{

        boolean danglingFlag = true;
        int count = 0;
        Node node = new Node();
        node.pageRank = 0.0;
        node.adjacencyList = new ArrayList<String>();
        node.danglingFlag = false;

        //iterating over the value list and performing actions based on whether node is dangling or not
        for(Node node1: value){

            if(!(node1.danglingFlag)){
                node.adjacencyList = node1.adjacencyList;
                danglingFlag = false;
            }
            if((!danglingFlag && count > 1 ) || node.adjacencyList.size()>0){
                context.getCounter(PageRankDriver.globalCounter.pageCount).increment(1);
                context.write(key, node);
                return ;
            }
        }

        if(danglingFlag){
            node.danglingFlag = true;
            context.write(key, node);
            context.getCounter(PageRankDriver.globalCounter.pageCount).increment(1);
        }
    }

}
