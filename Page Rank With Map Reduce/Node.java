/**
 * Created by amogh-hadoop on 2/20/17.
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Writable;

public class Node implements Writable {

    public double pageRank;
    public ArrayList<String> adjacencyList;
    public boolean contributeFlag;
    public boolean danglingFlag;

    //default constructor . Initialize everything to defaults
    public Node(){

        pageRank = 0;
        adjacencyList = new ArrayList<String>();
        danglingFlag = true;

    }


    public void write(DataOutput out) throws IOException {
        out.writeDouble(pageRank);
        out.writeInt(adjacencyList.size());
        for(int i = 0 ; i < adjacencyList.size(); i++){
            out.writeUTF(adjacencyList.get(i));
        }
        out.writeBoolean(contributeFlag);
        out.writeBoolean(danglingFlag);
    }


    public void readFields(DataInput in) throws IOException {

        pageRank = in.readDouble();
        int len = in.readInt();
        adjacencyList = new ArrayList<String>();
        for(int i = 0 ; i< len; i++){
            adjacencyList.add(in.readUTF());
        }
        contributeFlag = in.readBoolean();
        danglingFlag = in.readBoolean();

    }
}
