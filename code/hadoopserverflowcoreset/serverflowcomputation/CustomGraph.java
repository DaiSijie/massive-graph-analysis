package hadoopserverflowcoreset.serverflowcomputation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class CustomGraph {

    private final HashMap<Integer, ArrayList<RealEdge>> realNetwork;
    private final HashMap<Integer, ArrayList<ResidualEdge>> residualNetwork;
    private final HashMap<Integer, HashMap<Integer, RealEdge>> realEdges;
    private final HashMap<Integer, HashMap<Integer, ResidualEdge>> residualEdges;

    public void print(){
        System.out.println("Graph with following edges");
        for(Map.Entry<Integer, ArrayList<RealEdge>> entry : realNetwork.entrySet()){
            System.out.print(entry.getKey() + " ");
            for(RealEdge real : entry.getValue())
                System.out.print(" (" + real.to + ", " + real.flow + "/" + real.capacity + ")");
            System.out.println("");
        }
    }

    private CustomGraph(HashMap<Integer, ArrayList<RealEdge>> realNetwork, HashMap<Integer, ArrayList<ResidualEdge>> residualNetwork, HashMap<Integer, HashMap<Integer, RealEdge>> realEdges, HashMap<Integer, HashMap<Integer, ResidualEdge>> residualEdges){
        this.realNetwork = realNetwork;
        this.residualNetwork = residualNetwork;
        this.realEdges = realEdges;
        this.residualEdges = residualEdges;
    }

    public double getFlowOutOf(int vertex){
        double flow = 0;
        for(RealEdge e : realNetwork.get(vertex))
            flow += e.flow;
        return flow;
    }

    public void deleteVertex(int v){
        //find all incoming edges in the real graph
        HashSet<Integer> realIn = new HashSet<>();
        for(Map.Entry<Integer, ResidualEdge> entry : residualEdges.get(v).entrySet()){
            if(!entry.getValue().representsRealEdge)
                realIn.add(entry.getKey());
        }

        //delete all outgoing edges
        realNetwork.remove(v);
        residualNetwork.remove(v);
        realEdges.remove(v);
        residualEdges.remove(v);

        //delete all ingoing edges
        for(int u : realIn){
            realNetwork.get(u).removeIf(realEdge -> realEdge.to == v);
            residualNetwork.get(u).removeIf(residualEdge -> residualEdge.to == v);
            realEdges.get(u).remove(v);
            residualEdges.get(u).remove(v);
        }
    }

    // need newCapacity > oldCapacity!!!
    public void setCapacity(int from, int to, double newCapacity){
        RealEdge real = realEdges.get(from).get(to);
        residualEdges.get(from).get(to).weight += (newCapacity - real.capacity);
        real.capacity = newCapacity;
    }

    public void resetFlow(){
        for(Map.Entry<Integer, ArrayList<RealEdge>> entry: realNetwork.entrySet()){
            for(RealEdge edge: entry.getValue()){
                edge.flow = 0;
                residualEdges.get(edge.from).get(edge.to).weight = edge.capacity;
                residualEdges.get(edge.to).get(edge.from).weight = 0;
            }
        }
    }

    public ArrayList<ResidualEdge> residualNeighbors(int vertex){
        return residualNetwork.get(vertex);
    }

    public ArrayList<RealEdge> realNeighbors(int vertex){
        return realNetwork.get(vertex);
    }

    public RealEdge getRealEdge(int from, int to){
        return realEdges.get(from).get(to);
    }

    public ResidualEdge getResidualEdge(int from, int to){
        return residualEdges.get(from).get(to);
    }

    public static class Builder{

        private final HashMap<Integer, ArrayList<RealEdge>> realNetwork;
        private final HashMap<Integer, ArrayList<ResidualEdge>> residualNetwork;
        private final HashMap<Integer, HashMap<Integer, RealEdge>> realEdges;
        private final HashMap<Integer, HashMap<Integer, ResidualEdge>> residualEdges;

        public Builder(){
            realNetwork = new HashMap<>();
            residualNetwork = new HashMap<>();
            realEdges = new HashMap<>();
            residualEdges = new HashMap<>();
        }

        public void addEdge(int from, int to, double initialCapacity){
            RealEdge real = new RealEdge(from, to, initialCapacity, 0);
            ResidualEdge residualRepresenting = new ResidualEdge(from, to, initialCapacity, true);
            ResidualEdge residualShadow = new ResidualEdge(to, from, 0, false);

            if(!realNetwork.containsKey(from))
                realNetwork.put(from, new ArrayList<>());
            realNetwork.get(from).add(real);

            if(!residualNetwork.containsKey(from))
                residualNetwork.put(from, new ArrayList<>());
            residualNetwork.get(from).add(residualRepresenting);

            if(!residualNetwork.containsKey(to))
                residualNetwork.put(to, new ArrayList<>());
            residualNetwork.get(to).add(residualShadow);

            if(!realEdges.containsKey(from))
                realEdges.put(from, new HashMap<>());
            realEdges.get(from).put(to, real);

            if(!residualEdges.containsKey(from))
                residualEdges.put(from, new HashMap<>());
            residualEdges.get(from).put(to, residualRepresenting);

            if(!residualEdges.containsKey(to))
                residualEdges.put(to, new HashMap<>());
            residualEdges.get(to).put(from, residualShadow);
        }

        public CustomGraph build(){
            return new CustomGraph(realNetwork, residualNetwork, realEdges, residualEdges);
        }

    }

}