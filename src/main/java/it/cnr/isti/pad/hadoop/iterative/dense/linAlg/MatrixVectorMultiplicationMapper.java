package it.cnr.isti.pad.hadoop.iterative.dense.linAlg;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MatrixVectorMultiplicationMapper
        extends Mapper<LongWritable, DoubleVector, LongWritable, DoubleWritable> {

    private static final Log LOG = LogFactory.getLog(MatrixVectorMultiplicationMapper.class);

    protected DoubleVector b = new DoubleVector();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        int size = context.getConfiguration().getInt("matrixSize",-1);
        if (size==-1){
            LOG.error("Invalid matrix size");
            throw new ConfigurationRuntimeException("Invalid matrix size");
        }
        if (b.get()==null || b.size()!=size)
            b.set(new double[size]);
        FileSystem fs =  FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get("b");
        Path path = new Path(filename);
        FSDataInputStream inputStream = fs.open(path);
        b.fromString(inputStream);
        fs.close();
        super.setup(context);
    }

    private final DoubleWritable out = new DoubleWritable();

    @Override
    protected void map(LongWritable key, DoubleVector value, Context context) throws IOException, InterruptedException {
        System.out.println(value);
        System.out.println(b);
        double sum = value.product(b);
        out.set(sum);
        context.write(key, out);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
