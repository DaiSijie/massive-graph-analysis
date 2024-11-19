package hadoopserverflowcoreset.serverflowcomputation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * A clever restartable Ford Fulkerson implementation
 *
 * Supporting incremental flow increase
 */
public class MemoryFordFulkerson {

    private final CustomGraph graph;
    private final int source;
    private final int sink;

    private static final double TINY = 0.0001d;

    private HashSet<Integer> reachableFromSource;

    public MemoryFordFulkerson(CustomGraph graph, int source, int sink){
        this.graph = graph;
        this.source = source;
        this.sink = sink;
    }

    public void findMaxFlow(){
        while(doRound());
    }

    public HashSet<Integer> reachableFromSource(){
        return reachableFromSource;
    }

    // Those fields are here to avoid creating too many objects and making the gc overheat
    private final HashSet<Integer> discoveredVertices = new HashSet<>();
    private final LinkedList<Integer> toVisit = new LinkedList<>();
    private final HashMap<Integer, ResidualEdge> discoverer = new HashMap<>();
    private final ArrayList<ResidualEdge> discovererOfSink = new ArrayList<>();

    // returns true iff there were augmenting paths
    private boolean doRound(){
        // step 0: init
        discoveredVertices.clear();
        toVisit.clear();
        discoverer.clear();
        discovererOfSink.clear();

        // step 1: init BFS
        discoveredVertices.add(source);
        toVisit.addLast(source);

        //step 2: BFS
        while(toVisit.size() > 0){
            int v = toVisit.removeFirst();
            for(ResidualEdge edge: graph.residualNeighbors(v)){
                if(!discoveredVertices.contains(edge.to) && edge.weight > TINY){
                    if(edge.to == sink){
                        discovererOfSink.add(edge);
                    }
                    else{
                        discoverer.put(edge.to, edge);
                        discoveredVertices.add(edge.to);
                        toVisit.addLast(edge.to);
                    }
                }
            }
        }

        // remember reachable from source
        reachableFromSource = discoveredVertices;

        // step 3: backtrack to find augmenting paths
        return pushIfPossible(discoverer, discovererOfSink);
    }

    private final HashSet<Integer> visited = new HashSet<>();
    private final AugmentingPath aug = new AugmentingPath();


    // return true iff there were paths
    private boolean pushIfPossible(HashMap<Integer, ResidualEdge> discoverer, ArrayList<ResidualEdge> discovererOfSink){
        boolean toReturn = false;

        // remember visited vertices to have a vertex-disjoint set of edges
        visited.clear();

        for(ResidualEdge e: discovererOfSink){
            // step 0: initialize the path
            aug.edges.clear();
            double bottleneck = e.weight;

            // step 1 : initialize backtrack
            aug.edges.add(e);
            int visiting = e.from;
            boolean valid = true;

            // step 2: do backtrack
            while(visiting != source){
                if(visited.contains(visiting))
                    valid = false;

                visited.add(visiting);
                ResidualEdge parentEdge = discoverer.get(visiting);

                if(valid){ // if not valid, don't bother do this stuff
                    aug.edges.add(parentEdge);
                    bottleneck = Math.min(bottleneck, parentEdge.weight);
                }
                visiting = parentEdge.from;
            }

            if(valid){ // we've found a new vertex disjoint augmenting path!
                toReturn = true;
                aug.bottleneck = bottleneck;
                augmentFlow(aug);
            }
        }

        return toReturn;
    }

    private void augmentFlow(AugmentingPath augPath){
        for(ResidualEdge edge : augPath.edges){
            if(edge.representsRealEdge){
                graph.getRealEdge(edge.from, edge.to).flow += augPath.bottleneck;
                graph.getResidualEdge(edge.from, edge.to).weight -= augPath.bottleneck;
                graph.getResidualEdge(edge.to, edge.from).weight += augPath.bottleneck;
            }
            else{
                graph.getRealEdge(edge.to, edge.from).flow -= augPath.bottleneck;
                graph.getResidualEdge(edge.to, edge.from).weight += augPath.bottleneck;
                graph.getResidualEdge(edge.from, edge.to).weight -= augPath.bottleneck;
            }

        }

    }

}
