package cs480a1.yqiu.ngram;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Qiu on 2/27/2015.
 */
public class TextArrayWritable extends ArrayWritable implements WritableComparable<TextArrayWritable> {

    Text[] texts;

    public TextArrayWritable() {
        super(Text.class);
    }

    public TextArrayWritable(String[] strings) {
        super(Text.class);
        texts = new Text[strings.length];
        for (int i = 0; i < strings.length; i++) {
            texts[i] = new Text(strings[i]);
        }
        set(texts);
    }

    public Text[] getTexts() {
        return texts;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        for (int i = 0; i < texts.length; i++) {
            texts[i].write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        for (int i = 0; i < texts.length; i++) {
            texts[i].readFields(in);
        }
    }

    @Override
    public int compareTo(TextArrayWritable o) {
        int anotherYear = Integer.parseInt(o.get()[1].toString());
        int thisYear = Integer.parseInt(this.get()[1].toString());
        if (!o.get()[0].toString().equalsIgnoreCase(texts[0].toString())) {
            return texts[0].compareTo(o.getTexts()[0]);
        } else {
            return thisYear - anotherYear;
        }
    }

    public String toString() {
        String out = "";
        for (int i = 0; i < texts.length; i++) {
            out.concat(texts[i + 1].toString()).concat("\t");
        }
        return out;
    }
}
