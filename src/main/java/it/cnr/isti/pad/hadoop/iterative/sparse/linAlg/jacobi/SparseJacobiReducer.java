package it.cnr.isti.pad.hadoop.iterative.sparse.linAlg.jacobi;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleSparseVector;
import it.cnr.isti.pad.hadoop.iterative.sparse.linAlg.SparseMatrixVectorMultiplicationReducer;
import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class SparseJacobiReducer  extends SparseMatrixVectorMultiplicationReducer{
    private DoubleSparseVector errorVector = new DoubleSparseVector();
    private DoubleSparseVector oldX = new DoubleSparseVector();

    private static final Log LOG = LogFactory.getLog(SparseJacobiReducer.class);

    @Override
    protected void setup(Context context) throws IOException {
        // Read the matrix size from the from the configuration
        int size = context.getConfiguration().getInt("matrixSize",-1);
        if (size==-1){
            LOG.error("Invalid matrix size");
            throw new ConfigurationRuntimeException("Invalid matrix size");
        }
        FileSystem fs =  FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get("x");
        // Basically read all the values form the previous iteration and copy it to the new vector
        // So if some value is frozen is not lost
        oldX.setSize(size);
        Path path = new Path(filename);
        FSDataInputStream inputStream = fs.open(path);
        oldX.fromString(inputStream);
        x.setAll(oldX);
        errorVector.setSize(size);
        fs.close();
    }

    @Override
    protected void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) {
        //
        int index = (int) key.get();
        for (DoubleWritable value : values) {
            if (value.get()!=0.)
                x.setAll((int)key.get(), value.get());
            errorVector.setAll(index, Math.abs(value.get() - oldX.get(index)));
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        FileSystem fs =  FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get("error");
        Path path = new Path(filename);
        FSDataOutputStream outputStream = fs.create(path,true);
        errorVector.write(outputStream);
        outputStream.close();
    }
}

