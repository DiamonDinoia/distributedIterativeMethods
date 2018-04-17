package it.cnr.isti.pad.hadoop.iterative.sparse.linAlg.jacobi;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleSparseVector;
import it.cnr.isti.pad.hadoop.iterative.sparse.linAlg.SparseMatrixVectorMultiplicationMapper;
import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class SparseJacobiMapper extends SparseMatrixVectorMultiplicationMapper {

    private static final Log LOG = LogFactory.getLog(SparseJacobiMapper.class);
    private DoubleSparseVector x = new DoubleSparseVector();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // Get the matrix size from the context
        int size = context.getConfiguration().getInt("matrixSize",-1);
        if (size==-1){
            LOG.error("Invalid matrix size");
            throw new ConfigurationRuntimeException("Invalid matrix size");
        }
        // Read the solution vector from the previous iteration
        FileSystem fs =  FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get("x");
        Path path = new Path(filename);
        FSDataInputStream inputStream = fs.open(path);
        x.setSize(size);
        x.fromString(inputStream);
        inputStream.close();
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, DoubleSparseVector row, Context context) throws IOException, InterruptedException {
        // Perform the Jacobi algorithm
        double sum = row.product(x);
        sum -= (row.get((int)key.get()) * (x.get((int)key.get())));
        sum = b.get((int) key.get()) - sum;
        sum/=row.get((int)(key.get()));
        out.set(sum);
        if (sum!=0.)
            context.write(key, out);
    }
}
