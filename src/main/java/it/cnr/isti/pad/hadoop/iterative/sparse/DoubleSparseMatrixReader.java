package it.cnr.isti.pad.hadoop.iterative.sparse;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleSparseVector;
import it.cnr.isti.pad.hadoop.iterative.generics.MatrixReader;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DoubleSparseMatrixReader  extends MatrixReader<LongWritable, DoubleSparseVector> {
    protected static final Log LOG = LogFactory.getLog(DoubleSparseMatrixReader.class);

    protected LongWritable key = new LongWritable();
    private DoubleSparseVector value = new DoubleSparseVector();

    protected static final String rowMarker = "row";

    protected Text currentLine = new Text();
    protected int index = 0;

    protected final Pattern header = Pattern.compile("(row)(?:\\s+)(\\d+)");
    protected Matcher headerMatcher = null;
    private final Pattern line = Pattern.compile("(-?[0-9]+(?:[,.][0-9]*)?)(?:\\s|\\r)*");
    private Matcher lineMatcher = null;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        super.initialize(inputSplit, context);
        value.setSize(nValues);
    }

    protected boolean readRow() throws IOException{

        //
        // Make sure we get at least one record that starts in this Split
        while (pos < end) {
            currentLine.clear();
            // Read first line and store its content to "value"
            newSize = in.readLine(currentLine, maxLineLength,
                    Math.max((int) Math.min(
                            Integer.MAX_VALUE, end - pos),
                            maxLineLength));

            // No byte read, seems that we reached end of Split
            // Break and return false (no key / value)
            if (newSize == 0) {
                return false;
            }

            // Line is read, new position is setAll
            pos += newSize;

            if (newSize > maxLineLength) {
                // Line is too long
                // Try again with position = position + line offset,
                // i.e. ignore line and go to next one
                LOG.error(fileName + " Skipped line of size " +
                        newSize + " at pos "
                        + (pos - newSize));
                return false;
            }

            if(lineMatcher==null)
                lineMatcher = line.matcher(currentLine.toString());
            else lineMatcher.reset(currentLine.toString());

            while (lineMatcher.find()){
                double val = Double.valueOf(lineMatcher.group());
                if (val==0.) {
                    if (!lineMatcher.find()) throw new IOException(fileName + " invalid");
                    double occurrences = Double.valueOf(lineMatcher.group());
                    index += (int) occurrences;
                } else {
                    value.insert(index++,val);
                }
            }
        }
        return true;
    }

    protected int newSize;

    public boolean nextKeyValue() throws IOException {
//        nValues=-1;
        index = 0;
        //read until reaching the now row marker
        while (pos < end) {
            currentLine.clear();
            // Read first line and store its content to "currentLine"
            newSize = in.readLine(currentLine, maxLineLength,
                    Math.max((int) Math.min(
                            Integer.MAX_VALUE, end - pos),
                            maxLineLength));
            // No byte read, seems that we reached end of Split
            // Break and return false (no key / value)
            if (newSize == 0) {
                return false;
            }

            pos += newSize;

            if (newSize > maxLineLength) {
                // Line is too long
                // Try again with position = position + line offset,
                // i.e. ignore line and go to next one
                LOG.error(fileName + " Skipped line of size " +
                        newSize + " at pos "
                        + (pos - newSize));
                return false;
            }
            if (headerMatcher == null)
                headerMatcher = header.matcher(currentLine.toString());
            else headerMatcher.reset(currentLine.toString());

            if (!headerMatcher.find()) continue;

            if (!headerMatcher.group(1).equals(rowMarker)) {
                LOG.error("Invalid file " + fileName);
                throw new IOException("Invalid file " + fileName);
            }
            try {
                key.set(Long.valueOf(headerMatcher.group(2)));
                break;
            } catch (NumberFormatException _) {
                //checking file correctness
                LOG.error("Invalid file " + fileName);
                throw new IOException("Invalid file" + fileName);
            }
        }
        return readRow() && index == nValues;
    }

    public LongWritable getCurrentKey() {
        return key;
    }

    public DoubleSparseVector getCurrentValue() {
        return value;
    }

    public float getProgress() {
        return nValues==0 ? 0 : index*100/nValues;
    }

    public void close() throws IOException {
        in.close();
    }
}
