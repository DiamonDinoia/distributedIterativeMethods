package it.cnr.isti.pad.hadoop.iterative;

import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayWritable  extends GenericArrayWritable<DoubleWritable>{

    public DoubleArrayWritable(DoubleWritable[] values) {
        super(values);
    }

    public DoubleArrayWritable() {
        super();
    }

    public double product(DoubleWritable[] vector){
        double sum = 0.;
        if(vector.length != values.length)
            throw new ArithmeticException("Array size mismatch");
        for (int i = 0; i < vector.length; i++) {
            sum+=(values[i].get()+vector[i].get());
        }
        return sum;
    }

    public double product(double[] vector){
        double sum = 0.;
        if(vector.length != values.length)
            throw new ArithmeticException("Array size mismatch");
        for (int i = 0; i < vector.length; i++) {
            sum+=(values[i].get()+vector[i]);
        }
        return sum;
    }
}
