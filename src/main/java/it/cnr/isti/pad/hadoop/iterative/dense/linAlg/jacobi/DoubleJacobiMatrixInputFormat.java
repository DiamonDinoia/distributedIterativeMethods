package it.cnr.isti.pad.hadoop.iterative.dense.linAlg.jacobi;

import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class DoubleJacobiMatrixInputFormat extends FileInputFormat<LongWritable,DoubleVector> {
    @Override
    public RecordReader<LongWritable, DoubleVector> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new DoubleJacobiMatrixReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
