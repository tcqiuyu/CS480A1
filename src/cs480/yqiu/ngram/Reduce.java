package cs480.yqiu.ngram;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

/**
 * Created by yqiu on 2/23/15.
 */
public class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>{
}
