package hadoopserverflowcoreset.computation;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Mapper2 extends Mapper<Text, Text, IntWritable, Text> {

    private final static IntWritable UNIQUE_KEY = new IntWritable(1208); // used to select a unique reducer for the final step

    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(UNIQUE_KEY, new Text(key.toString() + " " + value.toString()));
    }

}
