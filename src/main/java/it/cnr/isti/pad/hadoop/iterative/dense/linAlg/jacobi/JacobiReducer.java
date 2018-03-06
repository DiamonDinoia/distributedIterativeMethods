package it.cnr.isti.pad.hadoop.iterative.dense.linAlg.jacobi;

import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.MatrixVectorMultiplicationReducer;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JacobiReducer  extends MatrixVectorMultiplicationReducer {

    private DoubleVector errorVector = new DoubleVector();
    private DoubleVector x = new DoubleVector();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSystem fs =  FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get("x");
        Path path = new Path(filename);
        FSDataInputStream inputStream = fs.open(path);
        x.readFields(inputStream);
        errorVector.set(new double[x.size()]);
        fs.close();
        super.setup(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        super.reduce(key, values, context);
        int index = (int) key.get();
        for (DoubleWritable value : values) {
            errorVector.set(index, Math.abs(value.get()-x.get(index)));
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
