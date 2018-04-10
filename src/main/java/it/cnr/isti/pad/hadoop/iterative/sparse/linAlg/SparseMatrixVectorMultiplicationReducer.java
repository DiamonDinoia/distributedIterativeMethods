package it.cnr.isti.pad.hadoop.iterative.sparse.linAlg;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleSparseVector;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import sun.plugin.dom.exception.InvalidStateException;

import java.io.IOException;

public class SparseMatrixVectorMultiplicationReducer  extends Reducer<LongWritable, DoubleWritable, NullWritable, DoubleSparseVector> {

    protected DoubleSparseVector x = new DoubleSparseVector();
    private static final Log LOG = LogFactory.getLog(SparseMatrixVectorMultiplicationReducer.class);


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        x.reset();
        x.setSize(context.getConfiguration().getInt("matrixSize",-1));
        if (x.size()==1) {
            LOG.error("Invalid matrix size");
            throw new InvalidStateException("Matrix size unknown");
        }
    }

    @Override
    protected void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        for (DoubleWritable value : values) {
            x.insert((int)key.get(),value.get());
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), x);
    }
}
