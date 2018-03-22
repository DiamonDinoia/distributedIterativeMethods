package it.cnr.isti.pad.hadoop.iterative.dense.linAlg.jacobi;


import it.cnr.isti.pad.hadoop.iterative.dataStructures.DoubleVector;
import it.cnr.isti.pad.hadoop.iterative.dense.DoubleMatrixReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class DoubleJacobiMatrixReader
        extends DoubleMatrixReader {

    private DoubleVector error = new DoubleVector();
    private double threshold;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        super.initialize(inputSplit, context);
        Configuration job = context.getConfiguration();
        threshold = job.getFloat("threshold", 0.f);
        FileSystem fs = FileSystem.get(job);
        final Path file = new Path( job.get("error"));
        FSDataInputStream fileIn = fs.open(file);
        error.readFields(fileIn);
        fileIn.close();
    }

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
                if(error.get((int)key.get()) < threshold)
                    return false;
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

}
