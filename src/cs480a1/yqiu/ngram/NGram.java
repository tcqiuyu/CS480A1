package cs480a1.yqiu.ngram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by Qiu on 2/25/2015.
 */
public class NGram {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("Program started!");
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "Count NGram");

        job.setJarByClass(NGram.class);
        job.setMapperClass(NGramMapper.class);
        job.setCombinerClass(NGramCombiner.class);
        job.setReducerClass(NGramReducer.class);

        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.maxsize", (long) (64 * 1024 * 1024));

        MultipleOutputs.addNamedOutput(job, "Unigram",
                TextOutputFormat.class, TextArrayWritable.class, IntArrayWritable.class);
        MultipleOutputs.addNamedOutput(job, "Bigram",
                TextOutputFormat.class, TextArrayWritable.class, IntArrayWritable.class);

        //input
        //FileInputFormat.setInputPathFilter(job, TXTFilter.class);
        FileInputFormat.addInputPaths(job, args[args.length - 2]);
        job.setInputFormatClass(MultipleBooksInputFormat.class);

        //output
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(TextArrayWritable.class);
        job.setOutputValueClass(IntArrayWritable.class);
        job.waitForCompletion(true);
    }
}
