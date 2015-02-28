package cs480a1.yqiu.ngram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Qiu on 2/25/2015.
 */
public class NGramMapper extends Mapper<Text, TextArrayWritable, TextArrayWritable, IntArrayWritable> {

    private Context context;
    private String releaseYearStr;

    @Override
    public void map(Text key, TextArrayWritable value, Context context) throws IOException, InterruptedException {
        this.context = context;

        String currentSentence = key.toString();
        releaseYearStr = value.toString();

        String[] words;
        words = currentSentence.split("\\s");

        doNGram(0, words);
        doNGram(1, words);

    }

    private void doNGram(int n, String[] words) throws IOException, InterruptedException {
        String[] newWords;
        if (n != 1) {
            newWords = new String[words.length + 1];
            newWords[0] = " ";
            for (int i = 0; i < words.length; i++) {
                newWords[i + 1] = words[i];
            }
            newWords[newWords.length - 1] = " ";
        } else {
            newWords = words;
        }

        for (int i = 0; i < newWords.length; i++) {
            String nGramStr = null;
            if (n != 1) {
                for (int j = 1; j < n; j++) {
                    nGramStr = nGramStr + "/t" + newWords[i + j];
                }
            }
            //replace all non-alphanumeric char
            if (nGramStr != null) {
                nGramStr = nGramStr.replaceAll("[^a-zA-Z0-9 ]", "");
            }
            //key = nGram + release year + filename
            String[] keyStr = new String[]{nGramStr, releaseYearStr, String.valueOf(n)};
            System.out.println("Mapped key is : " + keyStr);
            TextArrayWritable key = new TextArrayWritable(keyStr);
            IntWritable[] val = new IntWritable[]{new IntWritable(1), new IntWritable(1)};
            IntArrayWritable value = new IntArrayWritable(val);
            context.write(key, value);
        }


    }
}
