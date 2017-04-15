import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by amogh-hadoop on 2/23/17.
 */
public class PageRankCombiner extends Reducer<Text, Node, Text, Node>{



    public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {

        long pageCount = context.getConfiguration().getLong(PageRankDriver.pageCount, 1);

        //handling dangling node . the key for dangling node is @@@@
        if(key.toString().equals("@@@@")){
            double deltaSum = 0;
            for(Node v : values){
                deltaSum += v.pageRank;
            }

            Node deltaAcc = new Node();
            deltaAcc.adjacencyList = new ArrayList<String>();
            deltaAcc.pageRank = deltaSum;
            deltaAcc.contributeFlag = false;
            context.write(key,deltaAcc);
            return;
        }

        Node n = new Node();
        n.danglingFlag = true;
        double contributionAccumulator = 0.0;

        for(Node v : values){
            // adding up contribtions if the node is a contributer
            if(v.contributeFlag){
                contributionAccumulator += v.pageRank;
            }
            else{
                context.write(key,v);
            }

        }
        n.pageRank = contributionAccumulator;
        n.contributeFlag = true;
        n.adjacencyList= new ArrayList<String>();
        context.write(key,n);




    }



}
