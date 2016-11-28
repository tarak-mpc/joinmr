package join.mr;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinRightMapper extends Mapper<LongWritable, Text, CompositeKey, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] str = value.toString().split(",");
        CompositeKey rightKey = new CompositeKey(str[0], "2");
        context.write(rightKey, new Text(str[1]));
    }
}
