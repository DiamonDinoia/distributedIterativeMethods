package it.cnr.isti.pad.hadoop.iterative.sparse.linAlg;

import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleSparseVector;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SparseMatrixVectorMultiplicationMapper extends Mapper<LongWritable, DoubleSparseVector,
        LongWritable, DoubleWritable>{

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, DoubleSparseVector value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);
    }
}
