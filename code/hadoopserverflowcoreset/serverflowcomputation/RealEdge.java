package hadoopserverflowcoreset.serverflowcomputation;

public class RealEdge {

    public final int from;
    public final int to;
    public double capacity;
    public double flow;

    public RealEdge(int from, int to, double initialCapacity, double initialFlow){
        this.from = from;
        this.to = to;
        this.capacity = initialCapacity;
        this.flow = initialFlow;
    }

    @Override
    public String toString(){
        return "(" + from + ", " + to + ", " + flow + "/" + capacity + ")";
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
