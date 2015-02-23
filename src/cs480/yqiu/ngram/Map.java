package cs480.yqiu.ngram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Priority;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by yqiu on 2/23/15.
 */

public class Map extends Mapper<LongWritable, Text, NGramWritable, ArrayWritable> {

    private StringFilter filter = new StringFilter();
    private String year;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        Configuration conf = context.getConfiguration();
        Class<StringFilter> filterClass = (Class<StringFilter>) conf.getClass("map.filter", null);
        if (filterClass != null) {
            try {
                filter = filterClass.newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

    protected void map(LongWritable lineNum, Text text, OutputCollector<NGramWritable, ArrayWritable> collector, Reporter reporter) {

        String line = text.toString();

        if (line.startsWith("Release Date:")) {
            String[] date = line.split(" ");
            year = date[date.length];
        }
        if (line == "*** START OF THIS PROJECT GUTENBERG EBOOK FLATLAND ***") {

        }
    }

}
