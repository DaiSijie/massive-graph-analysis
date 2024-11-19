package hadoopserverflowcoreset.computation;

import hadoopserverflowcoreset.util.MaximumMatchingComputer;
import hadoopserverflowcoreset.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class MaximumMatchingReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // step 1: gather all edges in appropriate format List<Pair<Integer, Integer>>
        ArrayList<Pair<Integer, Integer>> inEdges = new ArrayList<>();
        for(Text t: values)
            inEdges.add(textToEdge(t));

        // step 2: compute and output a maximum matching
        MaximumMatchingComputer mmComputer = new MaximumMatchingComputer(inEdges);
        for(Pair<Integer, Integer> edge : mmComputer.ComputeMaximumMatching()) {
            context.write(new IntWritable(edge.o1), new IntWritable(edge.o2));
        }
    }

    private Pair<Integer, Integer> textToEdge(Text text){
        String[] splat = text.toString().split(" ");
        return new Pair<>(new Integer(splat[0]), new Integer(splat[1]));
    }

}
