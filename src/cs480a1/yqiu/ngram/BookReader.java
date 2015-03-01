package cs480a1.yqiu.ngram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * Created by Qiu on 2/26/2015.
 * Key is Text+Release Year, value is filename
 */
public class BookReader extends RecordReader<TextYearWritable, Text> {

    private CombineFileSplit files;
    private LineReader lineReader;
    private TextYearWritable key = new TextYearWritable();
    private Text value;

    private long start;
    private long end;
    private long currentPos;

    private IntWritable releaseYear;
    private String filename;
    private Text currentLine = new Text("");
    private String currentSentenceStr = "";
    private String remainLineStr = "";

    public BookReader(CombineFileSplit combineFileSplit) {
        this.files = combineFileSplit;
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration configuration = context.getConfiguration();
        Path path = split.getPath();
        filename = path.getName();
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

        prepareToScanBook();

    }

    private void prepareToScanBook() throws IOException {
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
        if (lineString.startsWith("Release Date") || startWithMonths(lineString)) {//e.g. October, 1998 or Release Date: July, 1991 or Release Date: August 11, 2004 [EBook #46]
//            String[] releaseDateString = lineString.split(" ");
            int commaPos = lineString.indexOf(",");
            String releaseYearStr = lineString.substring(commaPos + 2, commaPos + 6);
            int year = Integer.parseInt(releaseYearStr);
//            String[] valueStr = new String[]{releaseYearStr, filename};
//            value = new TextYearWritable(valueStr);valueStr
            releaseYear = new IntWritable(year);

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
        if (currentPos >= end) {
            return false;
        }

        //TODO:simple split by ".". Does not include abbr. case

        int remainLinePeriodPos = remainLineStr.indexOf(".");

        if (remainLinePeriodPos == -1) {//if remaining line does not have period, need to read a new line
            boolean flag = true;
            while (flag) {

                lineReader.readLine(currentLine);
                String currentLineStr = currentLine.toString();
                int periodPos = currentLineStr.indexOf(".");//period position

                if (currentLine.toString().contains("End of the Project Gutenberg") || currentLine.toString().contains("End of Project Gutenberg")) {
                    return false;
                }

                if (periodPos != -1) {//if current line has period
                    flag = false;
                    String strToConcat = currentLineStr.substring(0, periodPos);
                    currentSentenceStr = currentSentenceStr.concat(strToConcat);
                    remainLineStr = currentLineStr.substring(periodPos + 1).trim();
                    currentSentenceStr = currentSentenceStr.replaceAll("\\n", " ");
                    this.key = new TextYearWritable(new Text(currentSentenceStr), releaseYear);
                    currentSentenceStr = "";//reset sentence
                } else {//if current line does not have period, concat whole line to current sentence
                    currentSentenceStr = currentSentenceStr.concat(currentLine.toString());
                }
            }
        } else {//remaining line has period. (especially for handling the case that many short sentences with in a line) Don't need to read a new line here
            currentSentenceStr = currentSentenceStr.concat(remainLineStr.substring(0, remainLinePeriodPos));
            remainLineStr = remainLineStr.substring(remainLinePeriodPos + 1).trim();
            currentSentenceStr = currentSentenceStr.replaceAll("\\n", " ");
            this.key = new TextYearWritable(new Text(currentSentenceStr), releaseYear);
            currentSentenceStr = "";//reset sentence
            return true;
        }

        return true;
    }


    @Override
    public TextYearWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return new Text(filename);
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (currentPos - start) / (float) (end - start));
        }
    }

    @Override
    public void close() throws IOException {
        lineReader.close();
    }
}
