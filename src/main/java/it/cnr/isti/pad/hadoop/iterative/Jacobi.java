package it.cnr.isti.pad.hadoop.iterative;
import it.cnr.isti.pad.hadoop.iterative.dense.DoubleMatrixInputFormat;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.MatrixVectorMultiplicationMapper;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.MatrixVectorMultiplicationReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Jacobi {

    private static String b = "b";

    public static void main( String[] args ) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("b", b);

        Path output =new Path(args[1]);
        FileSystem hdfs = FileSystem.get(conf);

        // delete existing directory
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        Job job = new Job(conf, "Matrix");
        job.setJarByClass(Jacobi.class);

        job.setInputFormatClass(DoubleMatrixInputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(MatrixVectorMultiplicationMapper.class);
        job.setReducerClass(MatrixVectorMultiplicationReducer.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
