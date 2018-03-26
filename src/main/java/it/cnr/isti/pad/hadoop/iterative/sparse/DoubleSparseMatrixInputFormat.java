package it.cnr.isti.pad.hadoop.iterative.sparse;

import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleSparseVector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class DoubleSparseMatrixInputFormat extends FileInputFormat<LongWritable,DoubleSparseVector> {

    @Override
    public RecordReader<LongWritable, DoubleSparseVector> createRecordReader(InputSplit split, TaskAttemptContext context)
                                                                    throws IOException,InterruptedException{
            return new DoubleSparseMatrixReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename){
            return false;
    }
}