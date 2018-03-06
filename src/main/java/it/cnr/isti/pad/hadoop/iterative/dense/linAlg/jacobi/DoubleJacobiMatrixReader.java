package it.cnr.isti.pad.hadoop.iterative.dense.linAlg.jacobi;

import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import it.cnr.isti.pad.hadoop.iterative.dense.DoubleMatrixReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class DoubleJacobiMatrixReader
        extends DoubleMatrixReader {

    private DoubleVector errorVector = new DoubleVector();
    public final static String errorKey = "errorFileName";
    public final static String errorSize = "errorVectorSize";

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        Configuration job = context.getConfiguration();
        FileSystem fs =  FileSystem.get(job);
        String errorName = job.get(errorKey);
        if(errorName!=null){
            FSDataInputStream errorIn = fs.open(new Path(errorName));
            errorVector.readFields(errorIn);
            errorIn.close();
        } else {
            int size = job.getInt(errorSize,-1);
            if (size==-1){
                LOG.error("Invalid configuration");
                throw new InvalidJobConfException("Invalid size error vector");
            }
            errorVector.set(new double[size]);
            errorVector.setConst(1./(double)size);
        }

        super.initialize(inputSplit, context);
    }

    @Override
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
                if(errorVector.get((int)key.get()) < 0.){
                    return false;
                }
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
        if(!readRow()) return false;

        value.set(values);
        return index==nValues;
    }
}
