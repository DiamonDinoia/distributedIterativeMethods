package it.cnr.isti.pad.hadoop.iterative;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import it.cnr.isti.pad.hadoop.iterative.dense.DoubleMatrixInputFormat;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.jacobi.DoubleJacobiMatrixInputFormat;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.jacobi.JacobiMapper;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.jacobi.JacobiReducer;
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

public class Jacobi {

    private static final String b = "b";
    private static final String error = "error";
    private static final String x = "x";
    private static final String matrixSize = "matrixSize";
    private static final String threshold = "threshold";

    private static final float tolerance = 0.0000000000001f;

    public static void main( String[] args ) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("b", b);
        conf.set("error", error);
        conf.set("x",x);
        conf.setInt(matrixSize,4);
        conf.setFloat(threshold,tolerance);
        Path output =new Path(args[1]);
        FileSystem hdfs = FileSystem.get(conf);

//        DoubleVector solution = new DoubleVector(new double[]{0.6, 2.27272, -1.1, 1.875});
        DoubleVector solution = new DoubleVector(new double[]{0.0, 0., 0., 0.});
        FSDataOutputStream out =  hdfs.create(new Path(x));
        out.writeBytes(solution.toString());
        out.close();
        solution.set(new double[]{1000., 1000., 1000., 1000.});
        out =  hdfs.create(new Path(error));
        solution.write(out);
        out.close();

        for (int i = 0; i < 20; i++) {
            // delete existing directory
            if (hdfs.exists(output)) {
                hdfs.delete(output, true);
            }
            Job job = new Job(conf, "Matrix");


            job.setJarByClass(Jacobi.class);

            job.setInputFormatClass(DoubleJacobiMatrixInputFormat.class);

            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(DoubleWritable.class);

            job.setMapperClass(JacobiMapper.class);
            job.setReducerClass(JacobiReducer.class);


            job.setNumReduceTasks(1);

            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, output);

            job.waitForCompletion(true);
            hdfs.rename(new Path(args[1] + "/part-r-00000"), new Path("./x"));
        }
    }
}
