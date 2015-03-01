package cs480a1.yqiu.ngram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Qiu on 2/27/2015.
 */
public class TextYearWritable implements WritableComparable<TextYearWritable> {


    private Text text = new Text();
    private IntWritable year = new IntWritable();

    public TextYearWritable() {

    }

    public TextYearWritable(Text text, IntWritable year) {
//        texts = new Text[strings.length];
//        for (int i = 0; i < strings.length; i++) {
//            texts[i] = new Text(strings[i]);
//        }
//        set(texts);
        this.text = text;
        this.year = year;
    }

    public Text getText() {
        return text;
    }

    public IntWritable getYear() {
        return year;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        text.write(out);
        year.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        text = new Text();
        year = new IntWritable();

        text.readFields(in);
        year.readFields(in);
    }

    @Override
    public int compareTo(TextYearWritable other) {

        IntWritable anotherYear = other.getYear();

        if (!other.getText().toString().equalsIgnoreCase(text.toString())) {
            return text.compareTo(other.getText());
        } else {
            return year.compareTo(anotherYear);
        }
    }

    public String toString() {
        return text + "/t" + year;
    }
}
