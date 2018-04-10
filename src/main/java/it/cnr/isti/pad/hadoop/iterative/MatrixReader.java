package it.cnr.isti.pad.hadoop.iterative;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class MatrixReader<K,V> extends RecordReader<K,V>{

    protected static final Log LOG = LogFactory.getLog(it.cnr.isti.pad.hadoop.iterative.MatrixReader.class);

    private long start;
    protected long pos;
    protected long end;
    protected int maxLineLength;

    protected int nValues=0;

    protected LineReader in;
    protected String fileName;

    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        // This InputSplit is a FileInputSplit
        FileSplit split = (FileSplit) inputSplit;

        // Retrieve configuration, and Max allowed
        // bytes for a single record
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt(
                "mapred.linerecordreader.maxlength",
                Integer.MAX_VALUE);

        this.nValues = job.getInt("matrixSize",-1);
        if (nValues==-1){
            LOG.error("Invalid configuration please specify the size");
            throw new ConfigurationRuntimeException("Invalid size");
        }
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


    protected Text currentLine = new Text();


    protected int newSize;
    protected double[] values;
    protected final Pattern line = Pattern.compile("((-?[0-9]+(?:[,.][0-9]*)?)(?:\\s|\\r)*)");
    protected Matcher lineMatcher = null;
    protected int index = 0;


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

            // Line is read, new position is set
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
                values[index] = Double.valueOf(lineMatcher.group());
                index++;
            }
        }
        return true;
    }
}
