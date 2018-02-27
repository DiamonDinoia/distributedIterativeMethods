package it.cnr.isti.pad.hadoop.iterative.v2;

import it.cnr.isti.pad.hadoop.iterative.GenericArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MatrixVectorMultiplication
        extends Mapper<LongWritable, DoubleVector,LongWritable, DoubleVector>{

    @Override
    protected void map(LongWritable key, DoubleVector value, Context context)
            throws IOException, InterruptedException {
        context.write(key,value);
    }
}
