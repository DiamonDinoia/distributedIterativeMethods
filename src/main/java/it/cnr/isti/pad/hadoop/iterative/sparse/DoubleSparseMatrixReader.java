package it.cnr.isti.pad.hadoop.iterative.sparse;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import it.cnr.isti.pad.hadoop.iterative.MatrixReader;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleSparseVector;
import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.hsqldb.Record;

import javax.naming.ConfigurationException;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DoubleSparseMatrixReader
        extends MatrixReader<LongWritable, DoubleVector> {

    protected static final Log LOG = LogFactory.getLog(it.cnr.isti.pad.hadoop.iterative.dense.DoubleMatrixReader.class);

    protected LongWritable key = new LongWritable();
    protected DoubleSparseVector value = new DoubleSparseVector();

    protected static final String rowMarker = "row";


    protected final Pattern header = Pattern.compile("(row)(?:\\s+)(\\d+)");
    protected Matcher headerMatcher = null;


    public boolean nextKeyValue() throws IOException {
//        nValues=-1;
        index = 0;
        //read until reaching the now row marker
        while(pos < end){
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
            if(headerMatcher==null)
                headerMatcher = header.matcher(currentLine.toString());
            else headerMatcher.reset(currentLine.toString());

            if(!headerMatcher.find()) continue;

            if(!headerMatcher.group(1).equals(rowMarker)){
                LOG.error("Invalid file " + fileName);
                throw new IOException("Invalid file " + fileName);
            }
            try{
                key.set(Long.valueOf(headerMatcher.group(2)));
//                nValues = Integer.valueOf(headerMatcher.group(3));
                if(values==null || values.length!=nValues)
                    values = new double[nValues];
                break;
            } catch (NumberFormatException _){
                //checking file correctness
                LOG.error("Invalid file " + fileName);
                throw new IOException("Invalid file" + fileName);
            }
        }
        if(!readRow()) return false;
        value.set(values);
        return index==nValues;
    }

    public LongWritable getCurrentKey() {
        return key;
    }

    public DoubleVector getCurrentValue() {
        return value;
    }

    public float getProgress() {
        return nValues==0 ? 0 : index*100/nValues;
    }

    public void close() throws IOException {
        in.close();
    }
}
