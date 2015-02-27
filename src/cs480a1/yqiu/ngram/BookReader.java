package cs480a1.yqiu.ngram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * Created by Qiu on 2/26/2015.
 */
public class BookReader extends RecordReader<IntWritable, Text> {

    private LineReader lineReader;
    private IntWritable key = new IntWritable();
    private Text value = new Text();

    private long start;
    private long end;
    private long currentPos;

    private int releaseYear;
    private Text currentLine = new Text();

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration configuration = context.getConfiguration();
        Path path = split.getPath();
        FileSystem fileSystem = path.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path);
        lineReader = new LineReader(inputStream, configuration);

        //initial start point and end point
        start = split.getStart();
        end = start + split.getLength();

        inputStream.seek(start);
        if (start != 0) {
            start += lineReader.readLine(new Text(), 0, (int) Math.min(Integer.MAX_VALUE, end - start));
        }

        start += lineReader.readLine(currentLine);

        //get release year
        while (!containReleaseDate(currentLine)) {
            start += lineReader.readLine(currentLine);
        }

        //get book start line
        while (!bookStart(currentLine)) {
            start += lineReader.readLine(currentLine);
        }
        currentPos = start;
    }

    private boolean bookStart(Text text) {
        String lineString = text.toString();
        //two cases:
        if (lineString.startsWith("*END*THE SMALL PRINT!")) {//e.g. *END*THE SMALL PRINT! FOR PUBLIC DOMAIN ETEXTS*Ver.07.02.92*END*
            return true;
        } else if (lineString.startsWith("*** START OF THIS PROJECT")) {//e.g. *** START OF THIS PROJECT GUTENBERG EBOOK PETER PAN ***
            return true;
        }
        return false;
    }


    private boolean containReleaseDate(Text text) {
        String lineString = text.toString();
        //two cases:
        if (lineString.startsWith("Release Date")) {//e.g. October, 1998
            String[] releaseDateString = lineString.split(" ");
            releaseYear = Integer.parseInt(releaseDateString[releaseDateString.length]);
            return true;
        } else if (startWithMonths(lineString)) {//e.g. Release Date: July, 1991
            String[] releaseDateString = lineString.split(" ");
            releaseYear = Integer.parseInt(releaseDateString[releaseDateString.length]);
            return true;
        }
        return false;
    }

    private boolean startWithMonths(String lineString) {
        return lineString.startsWith("Janurary")
                || lineString.startsWith("Feburary")
                || lineString.startsWith("March")
                || lineString.startsWith("April")
                || lineString.startsWith("May")
                || lineString.startsWith("June")
                || lineString.startsWith("July")
                || lineString.startsWith("Auguest")
                || lineString.startsWith("September")
                || lineString.startsWith("October")
                || lineString.startsWith("November")
                || lineString.startsWith("December");
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (currentPos > end) {
            return false;
        }
//        currentPos += lineReader.readLine(currentLine);

        String remainLine = "";
        //TODO:simple split by ".". Does not include abbr. case
        if (!remainLine.contains(".")) {
            String line1 = remainLine;
            Text line2 = new Text();
            boolean flag = true;
            while (flag && ( != lineReader.readLine(line2))) {

            }

        }
        return false;
    }

    @Override
    public IntWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {

    }
}
