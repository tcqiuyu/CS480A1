package cs480a1.yqiu.ngram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by Qiu on 2/25/2015.
 */
public class NGramReducer extends Reducer<TextArrayWritable, IntArrayWritable, TextArrayWritable, IntArrayWritable> {

    private MultipleOutputs<TextArrayWritable, IntArrayWritable> multipleOutputs;

    @Override
    public void setup(Context context) {
        multipleOutputs = new MultipleOutputs<TextArrayWritable, IntArrayWritable>(context);
    }

    @Override
    public void reduce(TextArrayWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        int ngramOccur = 0;
        int volumeOccur = 0;

        for (IntArrayWritable val : values) {
            ngramOccur += val.get()[0].get();
            volumeOccur += val.get()[1].get();
        }

        IntWritable ngramCount = new IntWritable(ngramOccur);
        IntWritable volumeCount = new IntWritable(volumeOccur);

        IntArrayWritable outputValue = new IntArrayWritable(new IntWritable[]{ngramCount, volumeCount});

        String ngramStr = key.get()[0].toString();
        String yearStr = key.get()[1].toString();

        TextArrayWritable outputKey = new TextArrayWritable(new String[]{ngramStr, yearStr});

        System.out.println("reduce ket is : " + outputKey);
        if (key.get()[2].toString().matches("1")) {
            multipleOutputs.write(outputKey, outputValue, "Unigram");
        } else if (key.get()[2].toString().matches("2")) {
            multipleOutputs.write(outputKey, outputValue, "Bigram");
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
