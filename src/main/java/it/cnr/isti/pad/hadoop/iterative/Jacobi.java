package it.cnr.isti.pad.hadoop.iterative;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import it.cnr.isti.pad.hadoop.iterative.dense.DoubleMatrixInputFormat;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.MatrixVectorMultiplicationMapper;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.MatrixVectorMultiplicationReducer;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.jacobi.JacobiMapper;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.jacobi.JacobiReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

public class Jacobi {

    private static String b = "b";
    private static String error = "error";
    private static String x = "x";

    public static void main( String[] args ) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("b", b);
        conf.set("error", error);
        conf.set("x",x);

        Path output =new Path(args[1]);
        FileSystem hdfs = FileSystem.get(conf);

//        DoubleVector solution = new DoubleVector(new double[]{0.6, 2.27272, -1.1, 1.875});
        DoubleVector solution = new DoubleVector(new double[]{0.0, 0., 0., 0.});
        FSDataOutputStream out =  hdfs.create(new Path(x));
        solution.write(out);
        out.close();
        // delete existing directory
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        Job job = new Job(conf, "Matrix");

//        MultipleOutputs.addNamedOutput(job,"x", FileOutputFormat.class, NullWritable.class, DoubleVector.class);

        job.setJarByClass(Jacobi.class);

        job.setInputFormatClass(DoubleMatrixInputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(JacobiMapper.class);
        job.setReducerClass(JacobiReducer.class);


        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
