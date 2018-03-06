package it.cnr.isti.pad.hadoop.iterative.dense.linAlg.jacobi;

import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.MatrixVectorMultiplicationMapper;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

public class JacobiMapper extends MatrixVectorMultiplicationMapper {

    private final DoubleVector x = new DoubleVector();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSystem fs =  FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get("x");
        Path path = new Path(filename);
        FSDataInputStream inputStream = fs.open(path);
        x.readFields(inputStream);
        inputStream.close();
        filename = context.getConfiguration().get("b");
        path = new Path(filename);
        inputStream = fs.open(path);
        parseVector(b,inputStream);
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
