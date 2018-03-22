package it.cnr.isti.pad.hadoop.iterative.dataStructures;

import it.cnr.isti.pad.hadoop.iterative.generics.SparseVector;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DoubleSparseVector extends SparseVector<Double> {

    public DoubleSparseVector(int size, int nonZeros) {
        super(size, nonZeros);
    }

    public DoubleSparseVector(int size) {
        super(size);
    }

    public DoubleSparseVector() {
        super();
    }

    public int nonZeros(){
        return values.size();
    }

//    @Override
//    public String toString() {
//        StringBuilder builder = new StringBuilder();
//        Double out;
//        int lastIndex = 0;
//        for (Map.Entry<Integer, Double> entry : values.entrySet()) {
//            if (entry.getKey()!=lastIndex){
//
//            }
//        }
//    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(size);
        out.writeInt(values.size());
        for (Map.Entry<Integer, Double> entry : values.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeDouble(entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        size = in.readInt();
        int nonZeros = in.readInt();
        for (int i = 0; i < nonZeros; i++) {
            values.put(in.readInt(),in.readDouble());
        }
    }
}
