import java.io.IOException;
import java.util.StringTokenizer;
import java.io.InputStream;
import java.io.FileInputStream;
import org.apache.hadoop.io.FloatWritable;
import java.net.URI;
import java.util.*;
import java.io.*;

import java.util.Properties;
import java.io.FileReader;

import java.util.HashMap;
import java.io.BufferedReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;


public class Covid19_3 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, FloatWritable>{

    private final static FloatWritable one = new FloatWritable();
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      if (value.toString().contains("date") )
                return;
      StringTokenizer itr = new StringTokenizer(value.toString(),",");
      itr.nextToken();
      word.set(itr.nextToken());
      one.set(Float.parseFloat(itr.nextToken()));
      
      context.write(word, one);
      
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();
    private HashMap<String, Double> map 
            = new HashMap<>(); 

    public void setup(Context context)throws IOException, InterruptedException {
      try{
        FileSystem fs = FileSystem.get(context.getConfiguration());
    URI[] files = context.getCacheFiles();
    String line = "";
    String cvsSplitBy = ",";
    
    for (URI file : files){
      Path p = new Path(file.toString());
      BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
      while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] population = line.split(cvsSplitBy);
                if(line.contains("continent") || population.length!=5 || population[population.length-1].equals("")){
                  continue;
                } 
                map.put(population[1],Double.parseDouble(population[4]));
}
    }
  }
    catch(IOException ex) {
            System.err.println("Exception while reading stop words file: " + ex.getMessage());
        }

}


    public void reduce(Text key, Iterable<FloatWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      float sum = 0;
      float out=0;

      for (FloatWritable val : values) {
        sum += val.get();
      }
      try{
      out=(float) (sum/map.get(key.toString()))*1000000;
      }
      catch(Exception e){
        return;
      }
      result.set(out);
      context.write(key, result);

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Covid19_3");
    job.setJarByClass(Covid19_3.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    job.addCacheFile(new Path(args[1]).toUri());


    // Path OutputPath = new Path(args[1]);
    // OutputPath.getFileSystem(conf).delete(OutputPath, true);

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}