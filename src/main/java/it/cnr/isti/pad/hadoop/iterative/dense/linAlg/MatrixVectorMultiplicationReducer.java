package it.cnr.isti.pad.hadoop.iterative.dense.linAlg;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class MatrixVectorMultiplicationReducer extends Reducer<LongWritable, DoubleWritable, NullWritable, DoubleVector>{

    private static final Log LOG = LogFactory.getLog(MatrixVectorMultiplicationReducer.class);

    protected DoubleVector x = new DoubleVector();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Read the matrix size
        int size = context.getConfiguration().getInt("matrixSize",-1);
        if (size==-1){
            LOG.error("Invalid matrix size");
            throw new ConfigurationRuntimeException("Invalid matrix size");
        }
        if (x.get()==null || x.size()!=size)
            x.set(new double[size]);
        super.setup(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        for (DoubleWritable value : values) {
            x.set((int)key.get(), value.get());
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), x);
        super.cleanup(context);
    }
}
