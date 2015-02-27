package cs480a1.yqiu.ngram;

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
public class CombineBooksInputFormat extends CombineFileInputFormat {


    @Override
    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {

        return new CombineFileRecordReader((CombineFileSplit) split, context, CombineBooksReader.class);
    }


//    @Override
//    public List<InputSplit> getSplits(JobContext job) throws IOException {
//
//        List<InputSplit> splits = new ArrayList<InputSplit>();
//        for (Object o : listStatus(job)) {
//            //REVIEW: Can I Convert directly?
//            FileStatus file = (FileStatus) o;
//
//            Path path = file.getPath();
//            FileSystem fileSystem = path.getFileSystem(job.getConfiguration());
//
//            long length = file.getLen();
//
//            boolean bookStart = false;
//
//            if (bookStart) {
//
//            } else {
//
//            }
//
//        }
//
//        return null;
//    }
//
//    @Override
//    public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//        return null;
//    }


}
