/**
 * Created by amogh-hadoop on 2/21/17.
 */

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;


public class PreProcessMapper  extends Mapper<Object,Text,Text,Node> {

    private static Pattern namePattern;
    static{
        // Keep only html pages not containing tilde (~).
        namePattern = Pattern.compile("^([^~|\\?]+)$");
    }
    @Override
    public void map(Object key, Text value, Context context ){
        String line = value.toString();
        int delimLoc = line.indexOf(':');
        String pageName = line.substring(0, delimLoc);
        String html = line.substring(delimLoc + 1);
        Matcher matcher = namePattern.matcher(pageName);
        if (!matcher.find()) {
            // Skip this html file, name contains (~).
            return;
        }
        ArrayList<String> linkPageNames = new ArrayList<String>();

        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            SAXParser saxParser = spf.newSAXParser();
            XMLReader xmlReader = saxParser.getXMLReader();
            xmlReader.setContentHandler(new WikiParser(linkPageNames));
            xmlReader.parse(new InputSource(new StringReader(html)));
            Node node = new Node();

            node.pageRank = 0.0;
            node.adjacencyList = linkPageNames;

            node.danglingFlag = false;
            context.write(new Text(pageName), node);
            for(String nodeName : linkPageNames){
                Node dummy = new Node();
                dummy.adjacencyList = new ArrayList<String>();
                dummy.danglingFlag = true;
                context.write(new Text(nodeName), dummy);
            }

        } catch (Exception e) {
            return;
        }



    }


}
