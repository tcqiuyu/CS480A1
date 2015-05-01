package cs480a1.yqiu.ngram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
        job.getConfiguration().setLong("mapreduce.input.fileinputformat.split.minsize.per.node", 0);
        MultipleOutputs.addNamedOutput(job, "Unigram",
                TextOutputFormat.class, TextYearWritable.class, IntArrayWritable.class);
        MultipleOutputs.addNamedOutput(job, "Bigram",
                TextOutputFormat.class, TextYearWritable.class, IntArrayWritable.class);

        //input
//        FileInputFormat.addInputPaths(job, args[args.length - 2]);
        FileInputFormat.setInputPaths(job, new Path(args[args.length - 2]));
        FileInputFormat.setInputDirRecursive(job, true);
        job.setInputFormatClass(MultipleBooksInputFormat.class);

        //output
        FileSystem fs = FileSystem.get(conf);
        Path outputPath = new Path(args[args.length - 1]);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
            System.out.println("Output Path: \"" + outputPath.getName() + "\" exists. Deleted.");
        }
        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(TextYearWritable.class);
        job.setOutputValueClass(IntArrayWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
