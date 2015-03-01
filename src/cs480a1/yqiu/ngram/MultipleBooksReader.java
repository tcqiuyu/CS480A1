package cs480a1.yqiu.ngram;

import org.apache.hadoop.fs.Path;
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
public class MultipleBooksReader extends RecordReader<TextYearWritable, Text> {

    private int index;
    private BookReader bookReader;
    private int totalFiles;
    public MultipleBooksReader(CombineFileSplit split, TaskAttemptContext context, Integer index) throws Exception {
        super();
        this.index = index;
        throw (new IOException(index.toString()));
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        CombineFileSplit combineFileSplit = (CombineFileSplit) split;
        FileSplit fileSplit = new FileSplit(combineFileSplit.getPath(index), combineFileSplit.getOffset(index),
                combineFileSplit.getLength(), combineFileSplit.getLocations());
        Path[] paths = combineFileSplit.getPaths();
        totalFiles = paths.length;
        bookReader.initialize(fileSplit, context);
//        if (index > 1) {
//            throw (new IOException(combineFileSplit.getPath(index).getName()));
//        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return bookReader.nextKeyValue();
    }

    @Override
    public TextYearWritable getCurrentKey() throws IOException, InterruptedException {
        return bookReader.getCurrentKey();
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
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
