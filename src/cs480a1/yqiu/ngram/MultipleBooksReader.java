package cs480a1.yqiu.ngram;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by Qiu on 2/25/2015.
 */
public class MultipleBooksReader extends RecordReader<Text, TextArrayWritable> {

    private int index;
    private BookReader bookReader;

    public MultipleBooksReader(CombineFileSplit split, TaskAttemptContext context, Integer index) {
        this.index = index;
        this.bookReader = new BookReader();
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        CombineFileSplit combineFileSplit = (CombineFileSplit) split;
        long[] startsLoc = combineFileSplit.getStartOffsets();
        FileSplit fileSplit = new FileSplit(combineFileSplit.getPath(index), combineFileSplit.getOffset(index),
                combineFileSplit.getLength(), combineFileSplit.getLocations());

        bookReader.initialize(fileSplit, context);
//        bookReader.setFileStartLoc(startsLoc);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return bookReader.nextKeyValue();
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return bookReader.getCurrentKey();
    }

    @Override
    public TextArrayWritable getCurrentValue() throws IOException, InterruptedException {
        return bookReader.getCurrentValue();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return bookReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        bookReader.close();
    }
}
