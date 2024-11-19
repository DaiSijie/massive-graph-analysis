package hadoopserverflowcoreset.serverflowcomputation;

import hadoopserverflowcoreset.util.Pair;

import java.util.*;

public class Region {

    private final static double TINY = 0.0001d; //everything below is considered zero.

    public final Set<Integer> regionClients;
    public final Set<Integer> regionServers;
    public final Map<Pair<Integer, Integer>, Double> realisation;
    public final Set<Pair<Integer, Integer>> support;
    public final double serverLoad;

    public Region(CustomGraph graph, int source, double normalization, HashSet<Integer> sourceCut) {
        this.regionClients = new HashSet<>();
        this.regionServers = new HashSet<>();
        this.realisation = new HashMap<>();
        this.support = new HashSet<>();

        for (RealEdge e : graph.realNeighbors(source)) {
            if (!sourceCut.contains(e.to))
                continue;

            regionClients.add(e.to);
            for (RealEdge f : graph.realNeighbors(e.to)) {
                regionServers.add(f.to);
                Pair<Integer, Integer> pair = new Pair<>(f.from, f.to);
                if (f.flow >= TINY)
                    support.add(pair);
                realisation.put(pair, f.flow / normalization);
            }
        }

        this.serverLoad = regionClients.size() * 1d / regionServers.size();
    }


    public void printFull() {
        printIntermediate();
        System.out.println("=== REALISATION ===");
        for (Map.Entry<Pair<Integer, Integer>, Double> e : realisation.entrySet())
            System.out.println("(" + e.getKey().o1 + ", " + e.getKey().o2 + ") -> " + e.getValue());
    }

    public void printIntermediate() {
        printSimplified();
        System.out.println("Support: " + support);
    }

    public void printSimplified() {
        System.out.println("===========================================");
        System.out.println("Region with server load " + serverLoad);
        System.out.println(regionClients.size() + " clients and " + regionServers.size() + " servers.");
        System.out.println("Support of size " + support.size());
    }

    public String simpleString(){
        String toReturn = "===========================================\n";
        toReturn = toReturn + ("Region with server load " + serverLoad) + "\n";
        toReturn = toReturn + (regionClients.size() + " clients and " + regionServers.size() + " servers.") + "\n";
        toReturn = toReturn + ("Support of size " + support.size()) + "\n";
        toReturn = toReturn + "===========================================";
        return toReturn;
    }
}

