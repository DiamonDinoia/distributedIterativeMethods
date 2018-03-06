package it.cnr.isti.pad.hadoop.iterative.dense.linAlg;

import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MatrixVectorMultiplicationReducer extends Reducer<LongWritable, DoubleWritable, NullWritable, DoubleVector>{

    private final Pattern header = Pattern.compile("(\\d+)");
    private final Matcher headerMatcher = header.matcher("");

    private DoubleVector x = new DoubleVector();

    private void readSize(FSDataInputStream inputStream) throws IOException {
        Text line = new Text();
        LineReader in = new LineReader(inputStream);
        if (in.readLine(line)==0)
            throw new IOException("Invalid size file");
        headerMatcher.reset(line.toString());
        if(headerMatcher.find()){
            int size = Integer.valueOf(headerMatcher.group());
            if(x.get()==null || x.size() != size) x.set(new double[size]);
        } else throw new IOException("Invalid size file");
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSystem fs =  FileSystem.get(context.getConfiguration());
        String filename = context.getConfiguration().get("b");
        Path path = new Path(filename);
        FSDataInputStream inputStream = fs.open(path);
        readSize(inputStream);
        inputStream.close();
        super.setup(context);
    }

    @Override
    protected void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        for (DoubleWritable value : values) {
            x.set((int)key.get(), value.get());
        }
    }


    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(), x);
        super.cleanup(context);
    }
}
