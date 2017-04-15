/**
 * Created by amogh-hadoop on 2/24/17.
 */
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TopKReducer extends
        Reducer<NullWritable, Text, NullWritable, Text> {

    private TreeMap<TreeMapObject, Text> topKRecords = new TreeMap<TreeMapObject, Text>(new MapComparator());

    @Override

    public void reduce(NullWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {
        for (Text v : values) {
            String line = v.toString();
            int delimiterIndex = line.indexOf(',');
            String record = line.substring(0, delimiterIndex);
            double pageRankValue = Double.parseDouble(record);


            topKRecords.put(new TreeMapObject(pageRankValue,line), new Text(line));

            if (topKRecords.size() > 100) {
                //remove current lowest to maintain size of 100
                topKRecords.remove(topKRecords.firstKey());
            }
        }

        // Writing contents of Tree Map to output file
        for (Text t : topKRecords.descendingMap().values()) {
            context.write(NullWritable.get(), t);
        }
    }
}
