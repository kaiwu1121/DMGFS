package gfs.server.masterManager;


import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

public class ConsistentHash<T> {
    private final HashFunction hashFunction;
    private final int numberOfReplicas;
                                   
    private final SortedMap<Long, T> circle = new TreeMap<Long, T>();

    public ConsistentHash(HashFunction hashFunction, int numberOfReplicas,
            Collection<T> nodes) {
        this.hashFunction = hashFunction;
        this.numberOfReplicas = numberOfReplicas;
        for (T node : nodes)
            add(node);
    }

    public void add(T node) {
        for (int i = 0; i < numberOfReplicas; i++)

            circle.put(hashFunction.hash(node.toString() + i), node);
    }

    public void remove(T node) {
        for (int i = 0; i < numberOfReplicas; i++)
            circle.remove(hashFunction.hash(node.toString() + i));
    }


    public T get(Object key) {
        if (circle.isEmpty())
            return null;
        long hash = hashFunction.hash((String) key);
        if (!circle.containsKey(hash)) {
            SortedMap<Long, T> tailMap = circle.tailMap(hash);
            hash = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(hash);
    }

    public long getSize() {
        return circle.size();
    }
    
   
    public void testBalance(){
        Set<Long> sets  = circle.keySet();
        SortedSet<Long> sortedSets= new TreeSet<Long>(sets);
        for(Long hashCode : sortedSets){
            System.out.println(hashCode);
        }
        
        System.out.println("----each location 's distance are follows: ----");
      
        Iterator<Long> it = sortedSets.iterator();
        Iterator<Long> it2 = sortedSets.iterator();
        if(it2.hasNext())
            it2.next();
        long keyPre, keyAfter;
        while(it.hasNext() && it2.hasNext()){
            keyPre = it.next();
            keyAfter = it2.next();
            System.out.println(keyAfter - keyPre);
        }
    }
    
    public static void main(String[] args) {
        Set<String> nodes = new HashSet<String>();
        nodes.add("A");
        nodes.add("B");
        nodes.add("C");
        
        ConsistentHash<String> consistentHash = new ConsistentHash<String>(new HashFunction(), 2, nodes);
        consistentHash.add("D");
        
        System.out.println("hash circle size: " + consistentHash.getSize());
        System.out.println("location of each node are follows: ");
        consistentHash.testBalance();
    }
    
}