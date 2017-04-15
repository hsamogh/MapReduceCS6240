/**
 * Created by amogh-hadoop on 2/19/17.
 */


import java.io.StringReader;
import java.net.URLDecoder;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.util.ArrayList;

/** Parses a Wikipage, finding links inside bodyContent div element. */
public class WikiParser extends DefaultHandler {
    /** List of linked pages; filled by parser. */
    private List<String> linkPageNames;
    /** Nesting depth inside bodyContent div element. */
    private int count = 0;
    private static Pattern linkPattern;
    private static Pattern namePattern ;

    static {

        // Keep only html filenames ending relative paths and not containing tilde (~).
        linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
        namePattern = Pattern.compile("^([^~|\\?]+)$");
    }

    public WikiParser(List<String> linkPageNames) {
        super();
        this.linkPageNames = linkPageNames;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        super.startElement(uri, localName, qName, attributes);
        if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) {
            // Beginning of bodyContent div element.
            count = 1;
        } else if (count > 0 && "a".equalsIgnoreCase(qName)) {
            // Anchor tag inside bodyContent div element.
            count++;
            String link = attributes.getValue("href");
            if (link == null) {
                return;
            }
            try {
                // Decode escaped characters in URL.
                link = URLDecoder.decode(link, "UTF-8");
            } catch (Exception e) {
                // Wiki-weirdness; use link as is.
            }
            // Keep only html filenames ending relative paths and not containing tilde (~).
            Matcher matcher = linkPattern.matcher(link);
            if (matcher.find()) {
                linkPageNames.add(matcher.group(1));
            }
        } else if (count > 0) {
            // Other element inside bodyContent div.
            count++;
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        super.endElement(uri, localName, qName);
        if (count > 0) {
            // End of element inside bodyContent div.
            count--;
        }
    }

    public static String makeGraph(String line){
        int delimLoc = line.indexOf(':');
        String pageName = line.substring(0, delimLoc);
        String html = line.substring(delimLoc + 1);
        Matcher matcher = namePattern.matcher(pageName);
        if (!matcher.find()) {
            // Skip this html file, name contains (~).
            return null;
        }
        ArrayList<String> linkPageNames = new ArrayList<>();

        try {
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
            SAXParser saxParser = spf.newSAXParser();
            XMLReader xmlReader = saxParser.getXMLReader();
            xmlReader.setContentHandler(new WikiParser(linkPageNames));
            xmlReader.parse(new InputSource(new StringReader(html)));
            String adjList = linkPageNames.toString();
            return pageName + "######" + adjList.substring(1, adjList.length()-1);


        } catch (Exception e) {
            // Discard ill-formatted pages.
            return null;
        }
    }
}
