package cs480a1.yqiu.ngram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Pattern;

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
//        if (key.getText().toString().contains("How CAN I have done that?")) {
//            throw (new IOException(value.toString()));
//        }
    }

    private void doNGram(int n, String[] words) throws IOException, InterruptedException {


        ArrayList<String> newWordsArray = new ArrayList<String>();

//        String regex = ".*[a-zA-Z0-9]+.*";

        for (String word : words) {
            if (word.matches("^.*[a-zA-Z0-9].*$")) {
                newWordsArray.add(word);

            }
        }

        int wordCount = newWordsArray.size();
        String[] newWords = (String[]) newWordsArray.toArray(new String[wordCount]);

        if (n != 1) {//if not unigram, add space at sentence start and end
            String[] tmpStr = new String[newWords.length + 1];
            tmpStr[0] = " ";
            System.arraycopy(newWords, 0, tmpStr, 1, newWords.length);
            tmpStr[tmpStr.length - 1] = " ";
            newWords = tmpStr;
        }


        for (int i = 0; i < newWords.length + 1 - n; i++) {
            String nGramStr = newWords[i];

            //construct n gram: e.g. word1 + "\t" + word2. (no "/t" at end).

            if (n == 2) {
                nGramStr = nGramStr + "\t" + newWords[i + 1];
            }

            //replace all non-alphanumeric char
            nGramStr = nGramStr.replaceAll("[^a-zA-Z0-9\\t]", "").toLowerCase();

            //key = nGram phrase_filename + release year
            String outStr = nGramStr.concat("_").concat(filename.toString());
            Text out = new Text(outStr);
            TextYearWritable key = new TextYearWritable(out, releaseYear);
            //value = total count + volume occurance count
            IntWritable[] val = new IntWritable[]{new IntWritable(1), new IntWritable(1)};
            IntArrayWritable value = new IntArrayWritable(val);
            context.write(key, value);
//            throw (new IOException(key.toString()));

        }


    }
}
