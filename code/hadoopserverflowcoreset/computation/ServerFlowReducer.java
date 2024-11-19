package hadoopserverflowcoreset.computation;

import hadoopserverflowcoreset.serverflowcomputation.CustomServerFlowComputer;
import hadoopserverflowcoreset.serverflowcomputation.Region;
import hadoopserverflowcoreset.util.Pair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class ServerFlowReducer extends Reducer<IntWritable, Text, IntWritable, IntWritable> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // step 1: gather all edges in appropriate format List<Pair<Integer, Integer>>
        ArrayList<Pair<Integer, Integer>> inEdges = new ArrayList<>();
        HashSet<Integer> clients = new HashSet<>();
        HashSet<Integer> servers = new HashSet<>();
        for(Text t: values) {
            Pair<Integer, Integer> edge = textToEdge(t);
            clients.add(edge.o1);
            servers.add(edge.o2);
            inEdges.add(edge);
        }

        context.progress();

        // step 2: compute a server flow realisation
        CustomServerFlowComputer sfComputer = new CustomServerFlowComputer(clients, servers, inEdges, context);
        sfComputer.compute();

        // step 3: output the support of the realisation
        for(Region region : sfComputer.getRegions()){
            for(Pair<Integer, Integer> edge : region.support){
                context.write(new IntWritable(edge.o1), new IntWritable(edge.o2));
            }
        }
    }

    private Pair<Integer, Integer> textToEdge(Text text){
        String[] splat = text.toString().split(" ");
        return new Pair<>(new Integer(splat[0]), new Integer(splat[1]));
    }

}
