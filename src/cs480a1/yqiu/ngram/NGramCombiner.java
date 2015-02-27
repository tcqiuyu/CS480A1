package cs480a1.yqiu.ngram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Qiu on 2/27/2015.
 */
public class NGramCombiner extends Reducer<TextArrayWritable, IntArrayWritable, TextArrayWritable, IntArrayWritable> {

    @Override
    public void reduce(TextArrayWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {

        int occur = 0;

        for (IntArrayWritable val : values) {
            occur += val.get()[0].get();
        }

        IntWritable ngramOccur = new IntWritable(occur);
        IntWritable[] occurs = {ngramOccur, new IntWritable(1)};

        IntArrayWritable value = new IntArrayWritable(occurs);
        context.write(key, value);
    }

}
