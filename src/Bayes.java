package qi.hadoop.bayes;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Bayes extends Configured implements Tool {

    private static Map<String, Double> priorProbability = new HashMap<String,Double>();
    private static Map<String, Double> contingentProbability = new HashMap<String,Double>();
    private static Map<String, Integer> wordNumInCategory = new HashMap<String, Integer>();

    public static class BayesMapper extends Mapper<Text, BytesWritable, Text, Text> {
        
        private Text word = new Text();
        
        private static String[] meaningless = {"A", "a", "the", "an", "An", "in", "of", "from", "to", "on", "and", "The", "As", "as", "AND"};

        private static Vector<String> meaninglessWord;

        private static String[] categories;

        @Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			if (meaninglessWord == null) {
				meaninglessWord = new Vector<String> ();
                for (String item : meaningless){
                    meaninglessWord.add(item);
                }
			}
			super.setup(context);

            Configuration conf = context.getConfiguration();
            //Path doc

		}

    }
}