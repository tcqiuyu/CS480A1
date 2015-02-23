package cs480.yqiu.ngram;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Arrays;

/**
 * Created by yqiu on 2/23/15.
 */
public class NGramWritable extends ArrayWritable implements WritableComparable<NGramWritable> {


    public NGramWritable() {
        super(Text.class);
    }

    public void set(String... ngrams) {
        Text[] texts = new Text[ngrams.length];
        for (int i = 0; i < ngrams.length; i++) {
            texts[i] = new Text(ngrams[i]);
        }
    }

    @Override
    public String toString() {
        String out = "";
        for (Writable w : this.get()) {
            out += w.toString() + "\t";
        }
        return out;
    }

    @Override
    public int compareTo(NGramWritable o) {
        Writable[] w1 = this.get();
        Writable[] w2 = o.get();
        int cmp = 0;
        int len = w1.length;

        for (int i = 0; i < len - 1; i++) {
            if (cmp == 0) {
                cmp = ((Text) (w1[i])).compareTo((Text) (w2[i]));
            }
        }
        if (cmp == 0) {
            cmp = Integer.parseInt(w1[len].toString()) - Integer.parseInt(w2[len].toString());
        }
        return cmp;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof NGramWritable) {
            return this.compareTo((NGramWritable) other) == 0;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(this.get());
    }
}
