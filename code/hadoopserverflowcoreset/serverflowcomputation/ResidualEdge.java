package hadoopserverflowcoreset.serverflowcomputation;

public class ResidualEdge {

    public final int from;
    public final int to;
    public double weight;
    public final boolean representsRealEdge;

    public ResidualEdge(int from, int to, double weight, boolean representsRealEdge){
        this.from = from;
        this.to = to;
        this.weight = weight;
        this.representsRealEdge = representsRealEdge;
    }

    @Override
    public String toString(){
        return "(" + from + ", " + to + ", " + weight + ", " + representsRealEdge + ")";
    }

    @Override
    public int hashCode(){
        if(from > to)
            return from * to;
        else
            return - from * to;
    }

    @Override
    public boolean equals(Object other){
        if(!(other instanceof RealEdge))
            return false;

        int from2 = ((RealEdge) other).from;
        int to2 = ((RealEdge) other).to;

        return from2 == this.from && to2 == this.to;
    }

}