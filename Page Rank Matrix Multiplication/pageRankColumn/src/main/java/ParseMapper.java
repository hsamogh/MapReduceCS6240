import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.Writable;
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
import org.apache.hadoop.io.Text;


// Preprocessor Map Class
public class ParseMapper extends Mapper<Object, Text, Text, Node> {
	private static Pattern namePattern;
	static{
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~|\\?]+)$");
	}
	 int count = 0;
	public void setup(Context ctx){
		
	}
	
//	Input : Key(default), Entry from .BZ file
	public void map(Object key, Text value, Context context ){
		
		count ++;
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
			Node nodeValue = new Node();
			
			nodeValue.adjacencyList = linkPageNames;
			nodeValue.hasAdjacencyList = true;
			context.write(new Text(pageName), nodeValue);			
			for(String outlink : linkPageNames){
				Node contribution = new Node();
				contribution.adjacencyList = new ArrayList<String>();
				contribution.hasAdjacencyList = false;
				context.write(new Text(outlink), contribution);
			}

		} catch (Exception e) {
			// Discard ill-formatted pages.
			return;
		}
		
	}
	
	public void cleanup(Context ctx){
	}
}
