
import java.io.IOException;
import java.io.PrintWriter;

import java.util.*;

import org.apache.hadoop.fs.*;
import org,apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;

public class serializationData extends Configured implements Tool {

    static class serializationDataMapper extends
			Mapper<NullWritable, BytesWritable, Text, BytesWritable> {
		private Text filenameKey = new Text();

		// setup在task开始前调用，这里主要是初始化filenamekey
		@Override
		protected void setup(Context context) {
			InputSplit split = context.getInputSplit();
			String fileName = ((FileSplit) split).getPath().getName();
			String className = ((FileSplit) split).getPath().getParent().getName();
			filenameKey.set(className + "|" + fileName);
		}

		@Override
		public void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			context.write(filenameKey, value);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
		PrintWriter out = null;
		
		Path inputPath = new Path(conf.get("INPUTPATH"));
		Path outputPath = new Path(conf.get("OUTPUTPATH"));
		
		FileSystem train_file = inputPath.getFileSystem(conf);
		FileStatus[] train_tempList = train_file.listStatus(inputPath);
		String[] INPUT_PATH_TRAIN = new String[train_tempList.length];
		
		boolean flag = true;
		
		for (int i = 0; i < train_tempList.length; i++) {
			INPUT_PATH_TRAIN[i] = train_tempList[i].getPath().toString();
			if(conf.get("INPUTPATH").equals("hdfs://master:9000/input/country_data/train")) {
//				Utils.CLASSGROUP += train_tempList[i].getPath().getName() + "/";
				if (flag) {
					out = new PrintWriter(Utils.FILE);
					flag = false;
				}
				out.print(train_tempList[i].getPath().getName() + "/");
			}
			System.out.println("file：" + INPUT_PATH_TRAIN[i]);
//			 System.out.println("CLASSGROUP: "+ CLASSGROUP);
		}
		
		
//		Path outputPath = new Path(Utils.SEQUENCE_INPUT_TRAIN_DATA);
		FileSystem fs = outputPath.getFileSystem(conf);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}

		Job job = Job.getInstance(conf, "SmallFilesToserializationDataConverter");
		
		job.setJarByClass(SmallFilesToserializationDataConverter.class);
		job.setMapperClass(serializationDataMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(serializationDataOutputFormat.class);

		// 根据输入的训练集路径参数，来指定输入
		if (null != INPUT_PATH_TRAIN) {
			for (String path : INPUT_PATH_TRAIN) {
				WholeFileInputFormat.addInputPath(job, new Path(path));
			}
		}
//		WholeFileInputFormat.addInputPath(job, new Path(Utils.BASE_TRAINDATA_PATH));
		
		serializationDataOutputFormat.setOutputPath(job, outputPath);
		
		if (!flag)
			out.close();

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new SmallFilesToserializationDataConverter(),
				args);
		
		System.exit(exitCode);
	}
}