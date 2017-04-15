import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by amogh-hadoop on 2/23/17.
 */
public class PageRankMapper extends Mapper<Text,Node, Text, Node> {
    @Override
    public void map(Text key, Node value , Context context) throws IOException, InterruptedException {

        //reading variables
        int pageCount = context.getConfiguration().getInt(PageRankDriver.pageCount,1);
        boolean iterationFlag = context.getConfiguration().getBoolean(PageRankDriver.iterationFlag, false);
        double delta = context.getConfiguration().getDouble(PageRankDriver.delta,0);
        double contribution = 0;

        // page rank value for first iteration should be 1/|V|
        if(iterationFlag){
            contribution = 1.0/(pageCount);
        }
        else
        {
            //computing dangling node adjustment
            contribution = (value.pageRank+ 0.85 *(delta/pageCount));
        }
        value.pageRank = contribution;
        context.write(key, value);

        //Iterating over nodes in adjacency list to distribute page rank
        for(String name : value.adjacencyList){
            Node nodeCon = new Node();
            nodeCon.contributeFlag = true;
            nodeCon.adjacencyList = new ArrayList<String>();
            nodeCon.pageRank = contribution/value.adjacencyList.size();
            context.write(new Text(name), nodeCon);
        }

        // Handling dangling nodes
        if(value.adjacencyList.size() == 0){
            Node nodeCon = new Node();
            nodeCon.adjacencyList = new ArrayList<String>();
            if(iterationFlag){
                nodeCon.pageRank = 1.0/pageCount;
            }
            else{
                nodeCon.pageRank = value.pageRank + 0.85 * (delta/pageCount) ;
            }
            context.write(new Text("@@@@"), nodeCon);
        }




    }
}
