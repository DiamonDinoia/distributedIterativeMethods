package it.cnr.isti.pad.hadoop.iterative.dense;

import com.sun.jersey.core.spi.component.ioc.IoCComponentProcessor;
import com.sun.org.apache.xerces.internal.impl.xpath.regex.Match;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import it.cnr.isti.pad.hadoop.iterative.generics.DoubleArrayWritable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MatrixVectorMultiplicationMapper
        extends Mapper<LongWritable, DoubleVector, LongWritable, DoubleVector> {

    private double[] b = null;

    private void parseVector(FSDataInputStream inputStream) throws IOException {
        Text line = new Text();
        LineReader in = new LineReader(inputStream);
        final Pattern linePattern = Pattern.compile("((-?[0-9]+(?:[,.][0-9]*)?)(?:\\s|\\r)*)");
        final Pattern header = Pattern.compile("(\\d+)");
        in.readLine(line);
        final Matcher headerMatcher = header.matcher(in.toString());
        if(headerMatcher.find()){
            b = new double[Integer.valueOf(headerMatcher.group())];
        } else throw new IOException("Invalid file");
        line.clear();
        Matcher lineMatcher = linePattern.matcher("");
        int index=0;
        while (in.readLine(line)>0){
            lineMatcher.reset(line.toString());
            while (lineMatcher.find()){
                b[index++] = Double.valueOf(lineMatcher.group());
            }
        }
        if(index!=b.length) throw new IOException("invalid file");

    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSystem fs =  FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get("b");
        FSDataInputStream inputStream = fs.open(new Path(filename));
        super.setup(context);
    }

    @Override
    protected void map(LongWritable key, DoubleVector value, Context context) throws IOException, InterruptedException {

        super.map(key,value,context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
