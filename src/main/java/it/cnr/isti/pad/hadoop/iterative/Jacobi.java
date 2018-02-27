package it.cnr.isti.pad.hadoop.iterative;
import it.cnr.isti.pad.hadoop.iterative.dense.DoubleMatrixInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Jacobi {

    private static String b = "b";

    public static void main( String[] args ) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "Matrix");
        job.setJarByClass(Jacobi.class);

        conf.set("b", b);

        conf.setStrings("b", b);
        job.setInputFormatClass(DoubleMatrixInputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(ArrayWritable.class);

        job.setMapperClass(MatrixVectorMultiplication.class);


        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
