package cs480a1.yqiu.ngram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by Qiu on 2/27/2015.
 * All ngram with in a file come in to one combiner
 */
public class NGramCombiner extends Reducer<TextYearWritable, IntArrayWritable, TextYearWritable, IntArrayWritable> {

    @Override
    public void reduce(TextYearWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {

        //key no need contain filename here. Remove it.
        String nGramStr = key.getText().toString().split("_")[0];
        Text nGram = new Text(nGramStr);
//        throw (new IOException(key.toString()));

        //new key = ngram + release year
        TextYearWritable newKey = new TextYearWritable(nGram, key.getYear());

        int ngramOccurTemp = 0;

        //count ngram occurance in one file
        for (IntArrayWritable val : values) {
            ngramOccurTemp += val.get()[0].get();
        }

        IntWritable ngramOccur = new IntWritable(ngramOccurTemp);
        IntWritable[] occurs = {ngramOccur, new IntWritable(1)};


        IntArrayWritable value = new IntArrayWritable(occurs);

        context.write(newKey, value);

    }

}
