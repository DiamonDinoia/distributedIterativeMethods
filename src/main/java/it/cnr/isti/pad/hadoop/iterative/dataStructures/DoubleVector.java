package it.cnr.isti.pad.hadoop.iterative.dataStructures;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.LineReader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DoubleVector implements Writable{

    private double[] values;

    public DoubleVector(){}

    public DoubleVector(double[] values) {
        this.values = values;
    }

    public double[] get() {
        return values;
    }

    public void set(double[] values) {
        this.values = values;
    }

    public double get(int index) {
        return values[index];
    }

    public void set(int index, double value) {
        this.values[index] = value;
    }

    public int size(){
        if (values==null) return 0;
        return values.length;
    }

    public double product(double[] vector ) {
        if (values.length != vector.length) throw new ArithmeticException("Vector size mismatch");
        double sum = 0.;
        for (int i = 0; i < vector.length; i++) {
            sum += (vector[i] * values[i]);
        }
        return sum;
    }

    public double product(DoubleVector vector ) {
        if (values.length != vector.size()) throw new ArithmeticException("Vector size mismatch");
        double sum = 0.;
        for (int i = 0; i < vector.size(); i++) {
            sum += (vector.get(i) * values[i]);
        }
        return sum;
    }

    public void setConst(double constant){
        for (int i = 0; i < values.length; i++) {
            values[i]=constant;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.length);
        for (double value : values) {
            out.writeDouble(value);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        values = new double[in.readInt()];
        for (int i = 0; i < values.length; i++) {
            values[i] = in.readDouble();
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (double value : values) {
            builder.append(value).append(' ');
        }
        return builder.toString();
    }

    private final Pattern linePattern = Pattern.compile("((-?[0-9]+(?:[,.][0-9]*)?)(?:\\s|\\r)*)");
    private final Matcher lineMatcher = linePattern.matcher("");
    private final Text line = new Text();

    public void fromString(FSDataInputStream inputStream) throws IOException {
        final LineReader in = new LineReader(inputStream);
        int index=0;
        while (in.readLine(line)>0){
            lineMatcher.reset(line.toString());
            while (lineMatcher.find()){
                this.set(index++, Double.valueOf(lineMatcher.group()));
            }
            line.clear();
        }
        if(index!=this.size()) throw new IOException("invalid vector file");
    }
}
