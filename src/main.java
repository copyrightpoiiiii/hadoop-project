
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class main {
    public static void main(Strings[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("INPUTPATH","hdfs://master:9000/input/country_data/train");
        conf.set("OUTPUTPATH","hdfs://master:9000/SerializationTrainData");
    }
}

