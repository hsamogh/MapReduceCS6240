import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by amogh-hadoop on 2/23/17.
 */
public class PageRankReducer extends Reducer<Text, Node, Text, Node>{

    private static final double ALPHA = 0.15;
    public double pageRankSum;

    public void setup(Context context){
        pageRankSum = 0;
    }

    public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {

        long pageCount = context.getConfiguration().getLong(PageRankDriver.pageCount, 1);


        if(key.toString().equals("@@@@")){
            double deltaSum = 0;
            for(Node v : values){
                deltaSum += v.pageRank;
            }

            long deltaSum1 = Double.doubleToLongBits(deltaSum);
            context.getCounter(PageRankDriver.globalCounter.delta).setValue(deltaSum1);
            return;
        }

            Node n = new Node();
            n.danglingFlag = false;
            double contributionAccumulator = 0.0;

            for(Node v : values){
                // Page Meta data for Adjacency list
                if(!v.danglingFlag){
                    n.adjacencyList = v.adjacencyList;
                    continue;
                }
                //Accumulate Contributions
                contributionAccumulator += v.pageRank;

            }

            double pageRank = (ALPHA/pageCount) + ((1.0-ALPHA) * (contributionAccumulator));
            //Update Page rank and emit
            n.pageRank = pageRank;
            context.write(key, n);

            pageRankSum += pageRank;


    }

    public void cleanup(Context context){

        System.out.println("Total page count : " + pageRankSum);
    }


}
