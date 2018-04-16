package it.cnr.isti.pad.hadoop.iterative;

import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleSparseVector;
import it.cnr.isti.pad.hadoop.iterative.sparse.DoubleSparseMatrixInputFormat;
import it.cnr.isti.pad.hadoop.iterative.sparse.linAlg.jacobi.SparseJacobiMapper;
import it.cnr.isti.pad.hadoop.iterative.sparse.linAlg.jacobi.SparseJacobiReducer;
import it.cnr.isti.pad.hadoop.iterative.utils.MatrixGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import static java.lang.System.exit;

public class SparseJacobi {
    private static final String b = "b";
    private static final String error = "error";
    private static final String x = "x";
    private static final String matrixSize = "matrixSize";
    private static final String threshold = "threashold";

    private static final float tolerance = 0.0000000000001f;

    private static void generate_input(FileSystem hdfs, int size) throws IOException{
        MatrixGenerator generator = new MatrixGenerator();
        for (int i = 0; i < size; i++) {
            final DoubleSparseVector test = generator.generateSparseVector(size, 0.5, i);
            final Path testOut = new Path("./input/" + 0 + i);
            final FSDataOutputStream outputStream = hdfs.create(testOut,true);
            outputStream.writeBytes("row " + i + '\n');
            outputStream.writeBytes(test.toString());
            outputStream.close();
        }
        {
            final DoubleSparseVector test = generator.generateSparseVector(size, 0.5);
            final Path testOut = new Path("b");
            final FSDataOutputStream outputStream = hdfs.create(testOut, true);
            outputStream.writeBytes(test.toString());
            outputStream.close();
        }
        {
            final DoubleSparseVector test = new DoubleSparseVector(size);
            final Path testOut = new Path("x");
            final FSDataOutputStream outputStream = hdfs.create(testOut, true);
            outputStream.writeBytes(test.toString());
            outputStream.close();
        }
    }


    public static void main( String[] args ) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("b", b);
        conf.set("error", error);
        conf.set("x",x);
        conf.setInt(matrixSize,4);
        conf.setFloat(threshold, tolerance);
        Path output = new Path(args[1]);
        FileSystem hdfs = FileSystem.get(conf);

//        generate_input(hdfs,4);

        // delete existing output directory
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        Job job = new Job(conf, "SparseMatrix");

        job.setJarByClass(it.cnr.isti.pad.hadoop.iterative.SparseJacobi.class);

        job.setInputFormatClass(DoubleSparseMatrixInputFormat.class);


        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(SparseJacobiMapper.class);
        job.setReducerClass(SparseJacobiReducer.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);

        exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
