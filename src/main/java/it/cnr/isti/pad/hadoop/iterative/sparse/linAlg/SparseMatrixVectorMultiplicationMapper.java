package it.cnr.isti.pad.hadoop.iterative.sparse.linAlg;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleSparseVector;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.MatrixVectorMultiplicationMapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import sun.plugin.dom.exception.InvalidStateException;

import java.io.IOException;

public class SparseMatrixVectorMultiplicationMapper extends Mapper<LongWritable, DoubleSparseVector,
        LongWritable, DoubleWritable>{

    private static final Log LOG = LogFactory.getLog(SparseMatrixVectorMultiplicationMapper.class);

    protected final DoubleSparseVector b = new DoubleSparseVector();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        b.setSize(context.getConfiguration().getInt("matrixSize",-1));
        if (b.size()==-1) throw new InvalidStateException("matrix size unknown");
        final FileSystem fs =  FileSystem.get(context.getConfiguration());
        final String filename = context.getConfiguration().get("b");
        final Path path = new Path(filename);
        final FSDataInputStream inputStream = fs.open(path);
        b.fromString(inputStream);
        fs.close();
    }

    protected final DoubleWritable out = new DoubleWritable();

    @Override
    protected void map(LongWritable key, DoubleSparseVector value, Context context) throws IOException, InterruptedException {
        double sum = value.product(b);
        out.set(sum);
        if (sum!=0.)
            context.write(key, out);
    }
}
