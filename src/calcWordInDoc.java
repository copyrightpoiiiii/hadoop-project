
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org,apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class calcWordInDoc extends Mapper<LongWritable,Text,Text,IntWritable> {

    //匹配英文单词，(-[/sa-zA-Z])* 为了匹配人名等加有连字符的单词
    private static final Pattern PATTERN = Pattern.compile("[/sa-zA-Z]+(-[/sa-zA-Z])*");

    //record word
    private Text word = new Text();

    //record word occurences
    private IntWritable wordCount = new IntWritable(1);

    //meaningless words
    private static Vector<String> meaninglessWord = {"A", "a", "the", "an", "An", "in", "of", "from", "to", "on", "and", "The", "As", "as", "AND"}

    @Override
    public void map(Text key, BytesWritable value, Context context)
        throws IOException, InterruptedException {
        String line = new String(value.getBytes(),0,value.getLength());
        Matcher m = PATTERN.matcher(line);
        String category_file = key.toString().split("@");
        while(m.find()){
            String tword = m.group();
            if(!meaninglessWord.contains(tword)){
                this.word.set(category_file[0] + "@" + tword);
                context.write(this.word,this.wordCount)
            }
        }
    }

    
}
