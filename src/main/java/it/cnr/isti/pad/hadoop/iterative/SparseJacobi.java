package it.cnr.isti.pad.hadoop.iterative;

import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleSparseVector;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.jacobi.DoubleJacobiMatrixInputFormat;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.jacobi.JacobiMapper;
import it.cnr.isti.pad.hadoop.iterative.dense.linAlg.jacobi.JacobiReducer;
import it.cnr.isti.pad.hadoop.iterative.utils.MatrixGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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

    public static void main( String[] args ) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        conf.set("b", b);
        conf.set("error", error);
        conf.set("x",x);
        conf.setInt(matrixSize,4);
        conf.setFloat(threshold,tolerance);
        Path output =new Path(args[1]);
        FileSystem hdfs = FileSystem.get(conf);

        MatrixGenerator generator = new MatrixGenerator();

        DoubleSparseVector test = generator.generateSparseVector(16, 0.5);

        Path path = new Path("test");
        FSDataOutputStream outTest = hdfs.create(path);
        test.write(outTest);
        outTest.close();
        FSDataInputStream inTest = hdfs.open(path);
        DoubleSparseVector test2 = new DoubleSparseVector();
        test2.readFields(inTest);
        System.out.println(test);
        System.out.println(test2);
        exit(0);
//        DoubleVector solution = new DoubleVector(new double[]{0.6, 2.27272, -1.1, 1.875});
        DoubleVector solution = new DoubleVector(new double[]{0.0, 0., 0., 0.});
        FSDataOutputStream out =  hdfs.create(new Path(x));
        out.writeBytes(solution.toString());
        out.close();
        solution.set(new double[]{1000., 1000., 1000., 1000.});
        out =  hdfs.create(new Path(error));
        solution.write(out);
        out.close();
        // delete existing directory
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        Job job = new Job(conf, "Matrix");

        job.setJarByClass(it.cnr.isti.pad.hadoop.iterative.SparseJacobi.class);

        job.setInputFormatClass(DoubleJacobiMatrixInputFormat.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapperClass(JacobiMapper.class);
        job.setReducerClass(JacobiReducer.class);


        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, output);

        exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
