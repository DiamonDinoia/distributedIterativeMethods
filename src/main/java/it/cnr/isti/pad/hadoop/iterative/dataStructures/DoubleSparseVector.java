package it.cnr.isti.pad.hadoop.iterative.dataStructures;

import it.cnr.isti.pad.hadoop.iterative.generics.SparseVector;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DoubleSparseVector extends SparseVector<Double> {

    public DoubleSparseVector(int size, int nonZeros) {
        super(size, nonZeros);
    }

    public DoubleSparseVector(int size) {
        super(size);
    }

    public DoubleSparseVector() {
        super();
    }

    public int nonZeros(){
        return values.size();
    }

    public double product(DoubleSparseVector vector){
        double sum = .0;
        if (values.isEmpty()) return sum;
        for (Map.Entry<Integer, Double> entry : values.entrySet()) {
            sum += vector.get(entry.getKey())*entry.getValue();
        }
        return sum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(size);
        out.writeInt(values.size());
        for (Map.Entry<Integer, Double> entry : values.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeDouble(entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        size = in.readInt();
        int nonZeros = in.readInt();
        for (int i = 0; i < nonZeros; i++) {
            values.put(in.readInt(),in.readDouble());
        }
    }


    private static final Pattern linePattern = Pattern.compile("(-?[0-9]+(?:[,.][0-9]*)?)(?:\\s|\\r)*");
    private final  Matcher lineMatcher = linePattern.matcher("");
    private final Text line = new Text();

    public void fromString(FSDataInputStream inputStream) throws IOException{
        final LineReader in = new LineReader(inputStream);
        int index=0;
        while (in.readLine(line)>0) {
            lineMatcher.reset(line.toString());
            while (lineMatcher.find()) {
                double val = Double.valueOf(lineMatcher.group());
                if (val == 0.) {
                    if (!lineMatcher.find()) throw new IOException("invalid vector file");
                    double occurrences = Double.valueOf(lineMatcher.group());
                    index += (int) occurrences;
                } else values.put(index++, val);
            }
            line.clear();
        }
        if(index!=this.size()) throw new IOException("invalid vector file");
    }
}
