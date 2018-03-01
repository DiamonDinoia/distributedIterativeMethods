package it.cnr.isti.pad.hadoop.iterative.dense.linAlg;

import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MatrixVectorMultiplicationMapper
        extends Mapper<LongWritable, DoubleVector, LongWritable, DoubleWritable> {

    private final Pattern linePattern = Pattern.compile("((-?[0-9]+(?:[,.][0-9]*)?)(?:\\s|\\r)*)");
    private final Pattern header = Pattern.compile("(\\d+)");
    private final Matcher headerMatcher = header.matcher("");
    private final Matcher lineMatcher = linePattern.matcher("");

    protected double[] b = null;

    protected double[] parseVector(double[] vector, FSDataInputStream inputStream) throws IOException {
        Text line = new Text();
        LineReader in = new LineReader(inputStream);
        if (in.readLine(line)==0)
            throw new IOException("Invalid b file");
        headerMatcher.reset(line.toString());
        if(headerMatcher.find()){
            int size = Integer.valueOf(headerMatcher.group());
            if(vector==null || vector.length != size) vector = new double[size];
        } else throw new IOException("Invalid b file");
        line.clear();
        int index=0;
        while (in.readLine(line)>0){
            lineMatcher.reset(line.toString());
            while (lineMatcher.find()){
                vector[index++] = Double.valueOf(lineMatcher.group());
            }
        }
        if(index!=vector.length) throw new IOException("invalid b file");
        return vector;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSystem fs =  FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get("b");
        Path path = new Path(filename);
        FSDataInputStream inputStream = fs.open(path);
        b = parseVector(b, inputStream);
        fs.close();
        super.setup(context);
    }

    private final DoubleWritable out = new DoubleWritable();

    @Override
    protected void map(LongWritable key, DoubleVector value, Context context) throws IOException, InterruptedException {
        System.out.println(value);
        System.out.println(b);
        double sum = value.product(b);
        out.set(sum);
       context.write(key, out);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
