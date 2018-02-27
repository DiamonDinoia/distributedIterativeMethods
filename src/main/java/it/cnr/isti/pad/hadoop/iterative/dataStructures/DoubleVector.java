package it.cnr.isti.pad.hadoop.iterative.dataStructures;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleVector implements Writable{

    private double[] values;

    public DoubleVector(){}

    public DoubleVector(double[] values) {
        this.values = values;
    }

    public double[] get() {
        return values;
    }

    public void set(double[] values) {
        this.values = values;
    }

    public double get(int index) {
        return values[index];
    }

    public void set(int index, double value) {
        this.values[index] = value;
    }

    public double product(double[] vector ) {
        if (values.length != vector.length) throw new ArithmeticException("Vector size mismatch");
        double sum = 0.;
        for (int i = 0; i < vector.length; i++) {
            sum += (vector[i] * values[i]);
        }
        return sum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.length);
        for (double value : values) {
            out.writeDouble(value);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        values = new double[in.readInt()];
        for (int i = 0; i < values.length; i++) {
            values[i] = in.readDouble();
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (double value : values) {
            builder.append(value).append(' ');
        }
        return builder.toString();
    }
}
