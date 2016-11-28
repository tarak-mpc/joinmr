package join.mr;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class NaturalKeyGroupingComparator extends WritableComparator {
	
	protected NaturalKeyGroupingComparator(){
		super (CompositeKey.class, true);
	}
	
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2){
		CompositeKey c1 = (CompositeKey)w1;
		CompositeKey c2 = (CompositeKey)w2;
		
		return c1.getId().compareTo(c2.getId());
	}
}
