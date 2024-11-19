package hadoopserverflowcoreset.util;

import org.jgrapht.Graph;
import org.jgrapht.alg.interfaces.MatchingAlgorithm;
import org.jgrapht.alg.matching.HopcroftKarpMaximumCardinalityBipartiteMatching;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class MaximumMatchingComputer {

    private final List<Pair<Integer, Integer>> edgesInput;

    private HashSet<Integer> leftVertices;
    private HashSet<Integer> rightVertices;
    private Graph<Integer, DefaultEdge> graph;

    public MaximumMatchingComputer(List<Pair<Integer, Integer>> edges){
        this.edgesInput = edges;
        buildGraph();
    }

    public List<Pair<Integer, Integer>> ComputeMaximumMatching(){
        //step1: compute a MM
        HopcroftKarpMaximumCardinalityBipartiteMatching<Integer, DefaultEdge> alg =
                new HopcroftKarpMaximumCardinalityBipartiteMatching<>(graph, rightVertices, leftVertices);
        MatchingAlgorithm.Matching<Integer, DefaultEdge> result = alg.getMatching();

        //step2: return the MM
        ArrayList<Pair<Integer, Integer>> toReturn = new ArrayList<>();
        for(DefaultEdge e: result)
            toReturn.add(new Pair<>(graph.getEdgeSource(e), graph.getEdgeTarget(e)));

        return toReturn;
    }

    private void buildGraph(){
        this.leftVertices = new HashSet<>();
        this.rightVertices = new HashSet<>();
        this.graph = new SimpleGraph<>(DefaultEdge.class);

        for(Pair<Integer, Integer> e: edgesInput){
            leftVertices.add(e.o1);
            rightVertices.add(e.o2);
            graph.addVertex(e.o1);
            graph.addVertex(e.o2);
            graph.addEdge(e.o1, e.o2);
        }
    }

}

