package it.cnr.isti.pad.hadoop.iterative;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class GenericArrayWritable<T extends Writable> implements Writable{

    public GenericArrayWritable(T[] values) {
        this.values = values;
    }

    public GenericArrayWritable() {
    }

    protected T values[];

    public void set(T[] values) { this.values = values; }

    public T[] get() { return values; }


    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        values = (T[]) new Object[in.readInt()];          // construct values
        for (int i = 0; i < values.length; i++) {
            T value = (T) new Object();
            value.readFields(in);                       // read a value
            values[i] = value;                          // store it in values
        }
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(values.length);                 // write values
        for (Writable value : values) {
            value.write(out);
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (Writable doubleWritable: this.get()){
            builder.append(doubleWritable.toString()).append(" ");
        }
        return builder.toString();
    }

}
