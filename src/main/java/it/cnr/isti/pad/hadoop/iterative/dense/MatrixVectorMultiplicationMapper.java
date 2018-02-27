package it.cnr.isti.pad.hadoop.iterative.v2;

import it.cnr.isti.pad.hadoop.iterative.DoubleArrayWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MatrixVectorMultiplicationMapper
        extends Mapper<LongWritable, DoubleArrayWritable, LongWritable, DoubleArrayWritable> {

    private double[] b = null;

    private void parseVector(FSDataInputStream inputStream){

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSystem fs =  FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get("b");
        FSDataInputStream inputStream = fs.open(new Path(filename));
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, DoubleArrayWritable value, Context context) throws IOException, InterruptedException {
        super.map(key,value,context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
