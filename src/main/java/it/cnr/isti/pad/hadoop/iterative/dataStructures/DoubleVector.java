package it.cnr.isti.pad.hadoop.iterative.v2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleVector implements Writable{

    private double[] values;

    public DoubleVector(double[] values) {
        this.values = values;
    }

    public DoubleVector(){}

    public double[] getValues() {
        return values;
    }

    public void set(double[] values) {
        this.values = values;
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
