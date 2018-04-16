package it.cnr.isti.pad.hadoop.iterative.dataStructures;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.LineReader;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class implements a dense vector. For a sparse vector use SparseVector instead.
 */
public class DoubleVector implements Writable{

    private double[] values;

    /**
     * Initialized an empty vector. Values must be set subsequently
     */
    public DoubleVector(){}

    /**
     * Initialize a Vector from an array of values.
     * TODO: Remove because this is not a very good policy because it exposes the internal structure of the class.
     * @param values elements of the vector
     */
    public DoubleVector(double[] values) {
        this.values = values;
    }

    /**
     * TODO: remove because exposes the internal structure of the class
     * @return values of the vector.
     */
    public double[] get() {
        return values;
    }

    /**
     * Set the values of the vector from an array of values.
     * TODO: Perform a deep copy in order to avoid internal representation exposire
     * @param values
     */
    public void set(double[] values) {
        this.values = values;
    }

    /**
     * Return the element in position index
     * @param index coordinate of the element to retrieve
     * @return element in position index
     */
    public double get(int index) {
        return values[index];
    }

    /**
     * Set the element in position index to value
     * @param index coordinate of the element
     * @param value value to assign
     */
    public void set(int index, double value) {
        this.values[index] = value;
    }

    /**
     *
     * @return the size of the vector.
     */
    public int size(){
        if (values==null) return 0;
        return values.length;
    }

    /**
     *
     * @param vector input vector to multiply
     * @return Scalar product between two vector
     */
    public double product(double[] vector ) {
        if (values.length != vector.length) throw new ArithmeticException("Vector size mismatch");
        double sum = 0.;
        for (int i = 0; i < vector.length; i++) {
            sum += (vector[i] * values[i]);
        }
        return sum;
    }

    /**
     *
     * @param vector input vector to multiply
     * @return Scalar product between two vector
     */
    public double product(DoubleVector vector ) {
        if (values.length != vector.size()) throw new ArithmeticException("Vector size mismatch");
        double sum = 0.;
        for (int i = 0; i < vector.size(); i++) {
            sum += (vector.get(i) * values[i]);
        }
        return sum;
    }

    /**
     * Set all the values to constant
     * @param constant input value
     */
    public void setConst(double constant){
        for (int i = 0; i < values.length; i++) {
            values[i]=constant;
        }
    }

    /**
     * Serialize the vector to a DataOutput stream
     * @param out output stream
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(values.length);
        for (double value : values) {
            out.writeDouble(value);
        }
    }

    /**
     * Initialize the vector form a DataInput stream
     * @param in input stream
     * @throws IOException
     */
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

    /**
     * Read the vector from an InputStream performing the input validations
     * @param inputStream input stream
     * @throws IOException in case of invalid or corrupted file
     */
    public void fromString(InputStream inputStream) throws IOException {
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
