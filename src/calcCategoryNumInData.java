
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org,apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class calcCategoryNumInData extends Configured implements Tool {

    public static class calcCategoryNumInDataMapper extends Mapper<Text, IntWritable, Text, IntWritable> {

        private Text category = new Text();

        public void map(Text key, IntWritable value, Context context)
            throws IOException, InterruptedException {
                String[] line = key.toString().split("|");
                this.category.set(line[0]);
                context.write(this.category,new IntWritable(1));
            }
    }

    public static class calcCategoryNumInDataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private Text category = new Text();

        private IntWritable wordSum = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> value, Context context)
            throws  IOException, InterruptedException {
                int sum = 0;
                for(IntWritable item : values){
                    sum += item.get();
                }
                this.category.set(key);
                this.wordSum.set(sum);
                context.write(this.category,this.wordSum);
            }
    }

    @Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();
		
		Path outputPath = new Path("hdfs://master:9000/CategoryNum");
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		
		Job job = Job.getInstance(conf, "calcCategoryNumInData");
		
		job.setJarByClass(calcCategoryNumInData.class);
		job.setMapperClass(calcCategoryNumInDataMapper.class);
		job.setCombinerClass(calcCategoryNumInDataReducer.class);
		job.setReducerClass(calcCategoryNumInDataReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileInputFormat.addInputPath(job, new Path("hdfs://master:9000/SerializationTrainData"));
		
		SequenceFileOutputFormat.setOutputPath(job, outputPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {

		int res = ToolRunner.run(new Configuration(), new calcCategoryNumInData(), args);
		System.exit(res);
	}
}