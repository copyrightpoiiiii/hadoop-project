package qi.hadoop.bayes;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class main {

    public static void main(String[] args) throws Exception {
        //Configuration conf = new Configuration();
        //conf.set("INPUTPATH","hdfs://master:9000/input/country_data/train");
        //conf.set("OUTPUTPATH","hdfs://master:9000/SerializationTrainData");

        //serializationData seqCov = new serializationData();
        //ToolRunner.run(conf, seqCov, args);

        //calcCategoryNumInData calcCategoryNum = new calcCategoryNumInData();
        //ToolRunner.run(conf, calcCategoryNum, args);

        //calcWordInDoc calcWordInFile = new calcWordInDoc();
        //ToolRunner.run(conf, calcWordInFile, args);

        //calcWordNumInDoc calcWordNumInFile = new calcWordNumInDoc();
        //ToolRunner.run(conf, calcWordNumInFile, args);

        //Configuration conf_test = new Configuration();
        //conf_test.set("INPUTPATH","hdfs://master:9000/input/country_data/test");
        //conf_test.set("OUTPUTPATH","hdfs://master:9000/SerializationTrainData_test");

        //ToolRunner.run(conf_test, seqCov, args);

        Configuration confBayes = new Configuration();
        Bayes Bayes = new Bayes();
		BufferedReader in = new BufferedReader(new FileReader("group.txt"));
		String s;
		StringBuilder sb = new StringBuilder();
		while ((s = in.readLine()) != null) {
			sb.append(s);
		}
		in.close();
		System.out.println(sb.toString());
		confBayes.set("CLASSGROUP", sb.toString());
		ToolRunner.run(confBayes, Bayes, args);
        

    }
}

