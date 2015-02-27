package cs480a1.yqiu.ngram;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Created by Qiu on 2/25/2015.
 */
public class TXTFilter implements PathFilter {
    @Override
    public boolean accept(Path path) {
        String name = path.getName();
        //only process txt files.
        return name.endsWith("txt");
    }
}
