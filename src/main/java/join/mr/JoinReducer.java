package join.mr;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<CompositeKey, Text, Text, Text> {

	@Override
	protected void reduce(CompositeKey key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {


		Iterator<Text> iter = values.iterator();
		HashSet<String> first = new HashSet<String>();

		while (iter.hasNext()) {
			String sidevalue = iter.next().toString();
			if (key.getTag().equals("1")) {
				first.add(sidevalue);
			}
			if (key.getTag().equals("2")) {
				Iterator<String> itrF = first.iterator();
				while (itrF.hasNext()) {
					String lint = itrF.next();
					String row = lint + "," + sidevalue;
					context.write(new Text(row), new Text());
				}
			}
		}

	}
}


