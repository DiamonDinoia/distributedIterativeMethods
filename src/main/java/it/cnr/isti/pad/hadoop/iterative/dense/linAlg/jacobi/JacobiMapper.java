package it.cnr.isti.pad.hadoop.iterative.dense.linAlg.jacobi;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.MatrixVectorMultiplicationMapper;
import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class JacobiMapper extends MatrixVectorMultiplicationMapper {

    private final DoubleVector x = new DoubleVector();
    private static final Log LOG = LogFactory.getLog(JacobiMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        int size = context.getConfiguration().getInt("matrixSize",-1);
        if (size==-1){
            LOG.error("Invalid matrix size");
            throw new ConfigurationRuntimeException("Invalid matrix size");
        }
        if (x.get()==null || x.size()!=size)
            x.set(new double[size]);
        FileSystem fs =  FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get("x");
        Path path = new Path(filename);
        FSDataInputStream inputStream = fs.open(path);
        x.fromString(inputStream);
        inputStream.close();
        super.setup(context);
    }
    private final DoubleWritable out = new DoubleWritable();

    @Override
    protected void map(LongWritable key, DoubleVector row, Context context) throws IOException, InterruptedException {
        double sum = row.product(x);
        sum -= (row.get((int)key.get()) * (x.get((int)key.get())));
        sum = b.get((int) key.get()) - sum;
        sum/=row.get((int)(key.get()));
        out.set(sum);
        context.write(key, out);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
