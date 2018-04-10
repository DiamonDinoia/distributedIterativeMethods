package it.cnr.isti.pad.hadoop.iterative.utils;

import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleSparseVector;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;

import java.util.Random;

public class MatrixGenerator {
    private Random random;

    public MatrixGenerator(int seed) {
        this.random = new Random(seed);
    }

    public MatrixGenerator() {
        this.random = new Random(42);
    }

    public DoubleVector generateVector(int size, int index){
        final double[] values = new double[size];
        double sum = 0.;
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextDouble();
            sum+=Math.abs(values[i]);
        }
        values[index] = sum;
        return new DoubleVector(values);
    }

    public DoubleVector generateVector(int size){
        final double[] values = new double[size];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextDouble();
        }
        return new DoubleVector(values);
    }

    public DoubleSparseVector generateSparseVector(int size, double threshold, int index){
        final DoubleSparseVector sparseVector = new DoubleSparseVector(size);
        double sum = 0.;
        for (int i = 0; i < size; i++) {
            if (random.nextDouble()<threshold){
                double value = random.nextDouble();
                sum+=Math.abs(value);
                sparseVector.insert(i, value);
            }
        }
        sparseVector.insert(index,sum);
        return sparseVector;
    }

    public DoubleSparseVector generateSparseVector(int size, double threshold){
        final DoubleSparseVector sparseVector = new DoubleSparseVector(size);
        for (int i = 0; i < size; i++) {
            if (random.nextDouble()<threshold)
                sparseVector.insert(i, random.nextDouble());
        }
        return sparseVector;
    }
}
