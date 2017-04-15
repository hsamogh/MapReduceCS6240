
import org.apache.hadoop.io.ArrayWritable;


public class ContributionValueWritable extends ArrayWritable {
	
	public ContributionValueWritable() {
        super(ContributionValue.class);
    }
}
