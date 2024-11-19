package hadoopserverflowcoreset.serverflowcomputation;

import hadoopserverflowcoreset.util.Pair;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CustomServerFlowComputer {

    private final Set<Integer> clients;
    private final Set<Integer> servers;

    private final static int SOURCE_CODE = -1;
    private final static int SINK_CODE = -2;

    private final static double SINK_WEIGHT = 1d;
    private final static double MIDDLE_WEIGHT = 2d;

    private final static double INCREASE_FACTOR = 1.01d;
    private final static double EPSILON = 0.00001d;

    private double lambda;

    private final CustomGraph graph;
    private final ArrayList<Region> regions;

    private final Reducer.Context context;

    private final MemoryFordFulkerson mff;

    public CustomServerFlowComputer(Set<Integer> clients, Set<Integer> servers, List<Pair<Integer, Integer>> edges, Reducer.Context context) {
        if (hasIsolatedClients(clients, edges))
            throw new IllegalArgumentException("Computing a SF with isolated clients. Aborting.");

        CustomGraph.Builder builder = new CustomGraph.Builder();
        for (int c : clients)
            builder.addEdge(SOURCE_CODE, c, lambda);
        for (int s : servers)
            builder.addEdge(s, SINK_CODE, SINK_WEIGHT);
        for (Pair<Integer, Integer> e : edges)
            builder.addEdge(e.o1, e.o2, MIDDLE_WEIGHT);
        this.graph = builder.build();

        this.regions = new ArrayList<>();
        this.clients = clients;
        this.servers = servers;
        setFirstMinimumLambda();
        this.context = context;
        context.progress();

        this.mff = new MemoryFordFulkerson(graph, SOURCE_CODE, SINK_CODE);
    }

    public ArrayList<Region> getRegions() {
        return regions;
    }

    public void compute() {
        boolean binarySearch = true;
        while (graph.realNeighbors(SOURCE_CODE).size() > 0) {
            // find the critical region
            Region region = binarySearch ? findRegionBinary() : findNextRegionMultiplicative();
            if(lambda >= 0.01d){
                // heuristics: for small value of lambda, a binary search goes faster than the multiplicative one
                binarySearch = false;
            }

            // heuristic to get smaller stuff
            graph.resetFlow();
            mff.findMaxFlow();
            region = new Region(graph, SOURCE_CODE, lambda, mff.reachableFromSource());

            //inform on progress
            context.progress();

            //peel off the region from the graph
            peelOff(region);

            //add region
            regions.add(region);
        }
    }

    private void setFirstMinimumLambda() {
        // O(n) hack to find a not so bad first lower bound on the critical lambda
        int maxNeighborhood = 0;
        for (int s : servers) {
            if (graph.residualNeighbors(s).size() > maxNeighborhood) {
                maxNeighborhood = graph.residualNeighbors(s).size();
            }
        }
        lambda = 1d / maxNeighborhood;
    }

    private Region findRegionBinary() {
        // step1: find decent bounds for the binary search
        double lb = lambda * .99d; // the .99d is here for numerical stability
        double ub = (servers.size() * 1.01d) / (clients.size());

        // step2: default case to apply if ub - lb is too small
        if(ub - lb <= EPSILON){
            setSourceCapacity(ub, lambda);
            lambda = ub;
            mff.findMaxFlow();
            return new Region(graph, SOURCE_CODE, lambda, mff.reachableFromSource());
        }

        // step3: binary search
        while(ub - lb > EPSILON){
            double searchValue = (lb + ub) / 2;
            setSourceCapacity(searchValue, lambda);
            lambda = searchValue;

            mff.findMaxFlow();
            if(mff.reachableFromSource().size() == 1)
                lb = lambda;
            else
                ub = lambda;
        }

        // step4: finalize search
        if(mff.reachableFromSource().size() == 1){ //to avoid numerical disasters (i.e, distance was tiny but mid was still not large enough for cut to be non-trivial)
            setSourceCapacity(ub, lambda);
            lambda = ub;
            mff.findMaxFlow();
        }

        return new Region(graph, SOURCE_CODE, lambda, mff.reachableFromSource());
    }

    private void setSourceCapacity(double newCapacity, double oldCapacity){
        if(newCapacity < oldCapacity)
            graph.resetFlow();

        for (int c : clients)
            graph.setCapacity(SOURCE_CODE, c, newCapacity);
    }

    private Region findNextRegionMultiplicative() {
        mff.findMaxFlow();

        while (mff.reachableFromSource().size() <= 1) { ;
            // increase lambda and update the graph
            lambda *= INCREASE_FACTOR;

            for (int c : clients)
                graph.setCapacity(SOURCE_CODE, c, lambda);

            // re-compute a max flow
            mff.findMaxFlow();
        }

        return new Region(graph, SOURCE_CODE, lambda, mff.reachableFromSource());
    }

    private void peelOff(Region region) {
        for (int s : region.regionServers) {
            graph.deleteVertex(s);
            servers.remove(s);
        }
        for(int c : region.regionClients) {
            graph.deleteVertex(c);
            clients.remove(c);
        }
    }

    private boolean hasIsolatedClients(Set<Integer> clients, List<Pair<Integer, Integer>> edges){
        // check that there is no lonely client (could remove this later is graph is well constructed)
        HashSet<Integer> representedClients = new HashSet<>();

        for(Pair<Integer, Integer> e : edges)
            representedClients.add(e.o1);

        return clients.size() > representedClients.size();
    }

}
