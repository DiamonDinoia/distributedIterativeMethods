package it.cnr.isti.pad.hadoop.iterative.v2;

import it.cnr.isti.pad.hadoop.iterative.DoubleArrayWritable;
import it.cnr.isti.pad.hadoop.iterative.v2.DoubleMatrixReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class DoubleMatrixInputFormat  extends FileInputFormat<LongWritable,DoubleVector> {

    @Override
    public RecordReader<LongWritable, DoubleVector> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new DoubleMatrixReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
