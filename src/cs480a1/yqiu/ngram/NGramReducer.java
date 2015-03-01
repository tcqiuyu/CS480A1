package cs480a1.yqiu.ngram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/**
 * Created by Qiu on 2/25/2015.
 */
public class NGramReducer extends Reducer<TextYearWritable, IntArrayWritable, TextYearWritable, IntArrayWritable> {

    private MultipleOutputs<TextYearWritable, IntArrayWritable> multipleOutputs;

    @Override
    public void setup(Context context) {
        multipleOutputs = new MultipleOutputs<TextYearWritable, IntArrayWritable>(context);
    }

    @Override
    public void reduce(TextYearWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        int ngramOccur = 0;
        int volumeOccur = 0;

        //get accumulated count
        for (IntArrayWritable val : values) {
            ngramOccur += val.get()[0].get();
            volumeOccur += val.get()[1].get();
        }

        IntWritable ngramCount = new IntWritable(ngramOccur);
        IntWritable volumeCount = new IntWritable(volumeOccur);

        IntArrayWritable outputValue = new IntArrayWritable(new IntWritable[]{ngramCount, volumeCount});

        //unigram or bigram
        int ngramOption = key.getText().toString().trim().split("\t").length;

//        System.out.println("reduce ket is : " + outputKey);
        if (ngramOption == 1) {
            multipleOutputs.write(key, outputValue, "Unigram");
        } else if (ngramOption == 2) {
            multipleOutputs.write(key, outputValue, "Bigram");
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        multipleOutputs.close();
    }
}
