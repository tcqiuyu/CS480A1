package cs480a1.yqiu.ngram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

/**
 * Created by Qiu on 2/25/2015.
 */
public class NGram {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        Job job = Job.getInstance(conf, "Count NGram");

        job.setJarByClass(NGram.class);
        job.setMapperClass(NGramMapper.class);
        job.setCombinerClass(NGramCombiner.class);
        job.setReducerClass(NGramReducer.class);

        MultipleOutputs.addNamedOutput(job, "Unigram",
                TextOutputFormat.class, ArrayWritable.class, IntArrayWritable.class);
        MultipleOutputs.addNamedOutput(job, "Bigram",
                TextOutputFormat.class, ArrayWritable.class, IntArrayWritable.class);

        //input
        FileInputFormat.setInputPathFilter(job, TXTFilter.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        job.setInputFormatClass(MultipleBooksInputFormat.class);

        //output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(ArrayWritable.class);
        job.setOutputValueClass(IntArrayWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}