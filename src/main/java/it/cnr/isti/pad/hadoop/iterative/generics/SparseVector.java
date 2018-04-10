package it.cnr.isti.pad.hadoop.iterative.generics;

import org.apache.hadoop.io.Writable;
import sun.plugin.dom.exception.InvalidStateException;

import java.util.HashMap;
import java.util.Map;

public abstract class SparseVector<T extends Number> implements Writable{
    protected Map<Integer, T> values;
    protected int size = -1;

    public SparseVector(int size, int nonZeros) {
        this.values = new HashMap<>(nonZeros);
        this.size=size;
    }

    public SparseVector(int size) {
        this();
        this.size = size;
    }

    public SparseVector(){
        values = new HashMap<>();
    }

    public int size(){
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void reset(){
        size=-1;
        values.clear();
    }

    public T insert(int index, T value){
        return values.put(index, value);
    }

    public T get(int index){
        return values.get(index)!=null ? values.get(index) : (T) new Double(0.);
    }

    public T set(int index, T value){
        return values.put(index,value);
    }

    public void set(SparseVector<T> vector){
        this.size = vector.size;
        values.clear();
        values.putAll(vector.values);
    }

    @Override
    public String toString() {
        if (size<0) throw new InvalidStateException("Size unknown");
        final StringBuilder builder = new StringBuilder();
        if(values.size()==0)
           return builder.append(0.).append(' ').append(size).toString();
        int current = 0;
        for (Map.Entry<Integer, T> entry : values.entrySet()) {
            if (current != entry.getKey()) {
                builder.append(0.)
                        .append(" ")
                        .append(entry.getKey() - current)
                        .append(" ");
            }
            builder.append(entry.getValue())
                    .append(" ");
            current = entry.getKey() + 1;
        }
        if(current!=size){
            builder.append(.0).append(' ').append(size-current);
        }
        return builder.toString();
    }


}
