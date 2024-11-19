package hadoopserverflowcoreset.serverflowcomputation;

import java.util.ArrayList;

public class AugmentingPath {

    public ArrayList<ResidualEdge> edges;
    public double bottleneck;

    public AugmentingPath(){
        edges = new ArrayList<>();
    }

    @Override
    public String toString(){
        return "(edges: " + edges + ", bottleneck: " + bottleneck + ")";
    }

}