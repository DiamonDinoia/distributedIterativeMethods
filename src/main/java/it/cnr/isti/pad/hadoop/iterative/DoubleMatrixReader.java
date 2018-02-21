package it.cnr.isti.pad.hadoop.iterative;

import com.sun.org.apache.commons.logging.Log;
import com.sun.org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.Vector;

public abstract class DoubleMatrixReader
        extends RecordReader<LongWritable, ArrayWritable> {

    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private LongWritable key = new LongWritable();
    private ArrayWritable value = ArrayWritable(DoubleWritable.class);
    private static final Log LOG = LogFactory.getLog(TestReader.class);

    private static final String sep = " ";
    private static final String rowMarker = "row";

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

    Vector<Double> numbers = new Vector<>();
    public boolean nextKeyValue() throws IOException {

        int newSize = 0;

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
                break;
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
                break;
            }

            String[] words  = currentLine.toString().split(sep);
            if (words[0].equals(rowMarker)){
                key.set(Long.valueOf(words[1]));
                continue;
            }
            DoubleWritable[] numbers = new DoubleWritable[words.length];

            for (int i = 0; i < words.length; i++) {
                numbers[i] = new DoubleWritable(Double.valueOf(words[i]));
            }


        }


        if (newSize == 0) {
            // We've reached end of Split
            key = null;
            value = null;
            return false;
        } else {
            // Tell Hadoop a new line has been found
            // key / value will be retrieved by
            // getCurrentKey getCurrentValue methods
            return true;
        }
    }

    public LongWritable getCurrentKey() {
        return key;
    }

    public Text getCurrentValue() {
        return value;
    }

    public float getProgress() {
        return 0;
    }

    public void close() {

    }
}
