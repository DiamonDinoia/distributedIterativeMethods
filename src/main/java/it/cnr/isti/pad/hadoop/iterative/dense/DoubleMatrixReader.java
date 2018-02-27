package it.cnr.isti.pad.hadoop.iterative.v2;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import it.cnr.isti.pad.hadoop.iterative.DoubleArrayWritable;
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

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DoubleMatrixReader
        extends RecordReader<LongWritable, DoubleVector> {

    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key = new LongWritable();
    private DoubleVector value = new DoubleVector();
    private static final Log LOG = LogFactory.getLog(DoubleMatrixReader.class);

    private static final String rowMarker = "row";
    private String fileName;

    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        // This InputSplit is a FileInputSplit
        FileSplit split = (FileSplit) inputSplit;

        // Retrieve configuration, and Max allowed
        // bytes for a single record
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt(
                "mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);

        // Split "S" is responsible for all records
        // starting from "start" and "end" positions
        start = split.getStart();
        end = start + split.getLength();

        // Retrieve file containing Split "S"
        final Path file = split.getPath();
        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());

        fileName = split.getPath().getName();

        // If Split "S" starts at byte 0, first line will be processed
        // If Split "S" does not start at byte 0, first line has been already
        // processed by "S-1" and therefore needs to be silently ignored
        boolean skipFirstLine = false;
        if (start != 0) {
            skipFirstLine = true;
            // Set the file pointer at "start - 1" position.
            // This is to make sure we won't miss any line
            // It could happen if "start" is located on a EOL
            --start;
            fileIn.seek(start);
        }

        in = new LineReader(fileIn, job);

        // If first line needs to be skipped, read first line
        // and stores its content to a dummy Text
        if (skipFirstLine) {
            Text dummy = new Text();
            // Reset "start" to "start + line offset"
            start += in.readLine(dummy, 0,
                    (int) Math.min(
                            (long) Integer.MAX_VALUE,
                            end - start));
        }

        // Position is the actual start
        this.pos = start;
    }

    private Text currentLine = new Text();
    private int nValues=0;
    private int index = 0;


    private final Pattern header = Pattern.compile("(row)(?:\\s+)(\\d+)(?:\\s+)(\\d+)");
    private Matcher headerMatcher = null;
    private final Pattern line = Pattern.compile("((-?[0-9]+(?:[,.][0-9]*)?)(?:\\s|\\r)*)");
    private Matcher lineMatcher = null;


    private double[] values;
    public boolean nextKeyValue() throws IOException {
        nValues=-1;
        int newSize;
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
                LOG.error("Skipped line of size " +
                        newSize + " at pos "
                        + (pos - newSize));
                return false;
            }
            if(headerMatcher==null)
                headerMatcher = header.matcher(currentLine.toString());
            else headerMatcher.reset(currentLine.toString());

            if(!headerMatcher.find()) continue;

            if(!headerMatcher.group(1).equals(rowMarker)){
                LOG.error("Invalid file");
                throw new IOException("Invalid file");
            }
            try{
                key.set(Long.valueOf(headerMatcher.group(2)));
                nValues = Integer.valueOf(headerMatcher.group(3));
                if(values==null || values.length!=nValues)
                    values = new double[nValues];
                break;
            } catch (NumberFormatException _){
                //checking file correctness
                LOG.error("Invalid file");
                throw new IOException("Invalid file");
            }
        }

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

            // Line is read, new position is set
            pos += newSize;

            if (newSize > maxLineLength) {
                // Line is too long
                // Try again with position = position + line offset,
                // i.e. ignore line and go to next one
                LOG.error("Skipped line of size " +
                        newSize + " at pos "
                        + (pos - newSize));
                return false;
            }

            if(lineMatcher==null)
                lineMatcher = line.matcher(currentLine.toString());
            else lineMatcher.reset(currentLine.toString());

            while (lineMatcher.find()){
                    values[index] = Double.valueOf(lineMatcher.group());
                index++;
            }
        }
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
