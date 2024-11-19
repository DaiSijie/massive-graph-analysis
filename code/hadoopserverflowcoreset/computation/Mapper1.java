package hadoopserverflowcoreset.computation;

import hadoopserverflowcoreset.logic.HadoopServerFlowCoreset;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class Mapper1 extends Mapper<Object, Text, IntWritable, Text> {

    private Random random = new Random();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.write(getRandomReducerKey(), value);
    }

    private IntWritable getRandomReducerKey(){
        return new IntWritable(random.nextInt(HadoopServerFlowCoreset.NUMBER_OF_REDUCERS_1));
    }

}
