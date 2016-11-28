package join.mr;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinLeftMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] str = value.toString().split(",");
		CompositeKey leftKey = new CompositeKey(str[1], "1");
		context.write(leftKey, new Text(str[0]));
	}
}
