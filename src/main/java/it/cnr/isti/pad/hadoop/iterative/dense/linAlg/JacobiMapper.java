package it.cnr.isti.pad.hadoop.iterative.dense.linAlg;

import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JacobiMapper  extends MatrixVectorMultiplicationMapper{

    private double[] x = null;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSystem fs =  FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get("x");
        Path path = new Path(filename);
        FSDataInputStream inputStream = fs.open(path);
        x = parseVector(x, inputStream);
        fs.close();
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, DoubleVector value, Context context) throws IOException, InterruptedException {
        double sum = DoubleVector
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
