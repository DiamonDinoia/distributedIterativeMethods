package it.cnr.isti.pad.hadoop.iterative.generics;

import org.apache.hadoop.io.Writable;
import sun.plugin.dom.exception.InvalidStateException;

import java.util.HashMap;
import java.util.Map;

public abstract class SparseVector<T extends Number> implements Writable{
    protected Map<Integer, T> values;
    protected int size = -1;

    /**
     * Initialize the vector.
     * @param size vector dimension
     * @param nonZeros number of no zero elements if known
     */
    public SparseVector(int size, int nonZeros) {
        this.values = new HashMap<>(nonZeros);
        this.size=size;
    }

    /**
     *
     * @return the number of non zero elements of the vector
     */
    public SparseVector(int size) {
        this();
        this.size = size;
    }

    /**
     * Size must be specified subsequently otherwise it throws an error
     */
    public SparseVector(){
        values = new HashMap<>();
    }

    /**
     *
     * @return the size of the vector
     */
    public int size(){
        return size;
    }

    /**
     * Set the size of the vector to the specified number
     * @param size new size of the vector
     */
    public void setSize(int size) {
        this.size = size;
    }

    /**
     * Reset the status of the vector and deletes all the values
     */
    public void reset(){
        size=-1;
        values.clear();
    }

    /**
     * Inserts a nre element into the vector and returns the previous value
     * @param index Coordinate of insertion
     * @param value Value to insert
     * @return previous value or null
     */
    public T insert(int index, T value){
        return values.put(index, value);
    }

    /**
     * Returns the value in position index
     * @param index value position
     * @return value
     */
    public T get(int index){
        return values.get(index)!=null ? values.get(index) : (T) new Double(0.);
    }

    /**
     * Sets the value in position index and returns the previous value
     * @param index vector position
     * @param value value to setAll
     * @return previous value
     */
    public T setAll(int index, T value){
        T previous = values.put(index,value);
        if (previous==null){
            return (T) new Double(0.);
        }
        return previous;
    }

    /**
     * set all the parameters from a vector
     * @param vector source of the values
     */
    public void setAll(SparseVector<T> vector){
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
