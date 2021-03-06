package cs480a1.yqiu.ngram;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Qiu on 2/25/2015.
 */
public class IntArrayWritable implements WritableComparable {


    private IntWritable[] values;

    public IntArrayWritable() {
        values = new IntWritable[0];
    }

    public IntArrayWritable(IntWritable[] values) {
        this.values = values;
    }

    public IntWritable[] get() {
        return values;
    }

    @Override
    public int compareTo(Object o) {
        if (!(o instanceof IntArrayWritable)) {
            throw new ClassCastException();
        }
        IntArrayWritable other = (IntArrayWritable) o;
        IntWritable[] otherArray = other.get();
        int compareToVal = -1;
        for (int i = 0; i < this.values.length; ++i) {
            compareToVal = otherArray[i].compareTo(this.values[i]);
        }
        return compareToVal;
    }

    public String toString() {
        String out = "";
        for (IntWritable intWritable : this.values) {
            out += intWritable + "\t";
        }
        return out;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.values.length);

        for (IntWritable intWritable : values) {
            intWritable.write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.values = new IntWritable[in.readInt()];

        for (int i = 0; i < this.values.length; ++i) {
            IntWritable value = new IntWritable();
            value.readFields(in);
            this.values[i] = value;
        }
    }

}