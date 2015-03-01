package cs480a1.yqiu.ngram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Qiu on 2/25/2015.
 */
public class NGramMapper extends Mapper<TextYearWritable, Text, TextYearWritable, IntArrayWritable> {

    private Context context;
    private IntWritable releaseYear;
    private Text filename;

    @Override
    public void map(TextYearWritable key, Text value, Context context) throws IOException, InterruptedException {
        this.context = context;
        this.filename = value;

        String currentSentence = key.getText().toString();
        releaseYear = key.getYear();
        String[] words;
        words = currentSentence.split("\\s");

        IntWritable[] test = {new IntWritable(1), new IntWritable(1)};
//        context.write(key, new IntArrayWritable(test));
//        throw (new IOException());
        doNGram(1, words);
        doNGram(2, words);
        throw (new IOException(key.toString()));
    }

    private void doNGram(int n, String[] words) throws IOException, InterruptedException {
        String[] newWords;
        if (n != 1) {//if not unigram, add space at sentence start and end
            newWords = new String[words.length + 1];
            newWords[0] = " ";
            System.arraycopy(words, 0, newWords, 1, words.length);
            newWords[newWords.length - 1] = " ";
        } else {
            newWords = words;
        }


        for (int i = 0; i < newWords.length + 1 - n; i++) {
            String nGramStr = newWords[i];
            //construct n gram: e.g. word1 + "\t" + word2. (no "/t" at end).
            if (n == 2) {
//                for (int j = 1; j < n; j++) {
                    nGramStr = nGramStr + "\t" + newWords[i + 1];
//                }
            }

            //replace all non-alphanumeric char
            if (nGramStr != null) {
                nGramStr = nGramStr.replaceAll("[^a-zA-Z0-9 ]", "").toLowerCase();
            }

            //key = nGram phrase_filename + release year
            assert nGramStr != null;
            String outStr = nGramStr.concat("_").concat(filename.toString());
            Text out = new Text(outStr);
            TextYearWritable key = new TextYearWritable(out, releaseYear);
            //value = total count + volume occurance count
            IntWritable[] val = new IntWritable[]{new IntWritable(1), new IntWritable(1)};
            IntArrayWritable value = new IntArrayWritable(val);
            context.write(key, value);
        }


    }
}
