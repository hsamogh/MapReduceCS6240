import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by amogh-hadoop on 2/25/17.
 */
public class PreProcessCombiner extends Reducer<Text, Node, Text, Node> {

    public void reduce(Text key, Iterable<Node> value, Context context) throws IOException, InterruptedException{

        boolean danglingFlag = true;
        Node node = new Node();
        node.pageRank = 0.0;
        node.adjacencyList = new ArrayList<String>();
        node.danglingFlag = false;
        //Iterating over value list and performing appropriate action based on nature of node

        for(Node node1: value){
            if(!(node1.danglingFlag)){
                node.adjacencyList = node1.adjacencyList;
                danglingFlag = false;
            }
            if(!danglingFlag || node.adjacencyList.size()>0){
                context.write(key, node);
                return ;
            }
        }
        if(danglingFlag){
            node.danglingFlag = true;
            context.write(key, node);
        }
    }

}
