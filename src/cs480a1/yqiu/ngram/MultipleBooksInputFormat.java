package cs480a1.yqiu.ngram;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Created by Qiu on 2/25/2015.
 */
public class MultipleBooksInputFormat extends CombineFileInputFormat<TextYearWritable, Text> {


    @Override
    public RecordReader<TextYearWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {

        CombineFileSplit combineFileSplit = (CombineFileSplit) split;
        CombineFileRecordReader<TextYearWritable, Text> recordReader = new CombineFileRecordReader<TextYearWritable, Text>(combineFileSplit, context, MultipleBooksReader.class);
        try {
            recordReader.initialize(combineFileSplit, context);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return recordReader;
    }


}
