import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Covid19_1 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable();
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      if (value.toString().contains("date") )
                return;
      Configuration conf = context.getConfiguration();
      String param = conf.get("wor");
      StringTokenizer itr = new StringTokenizer(value.toString(),",");
      itr.nextToken();
      word.set(itr.nextToken());
      one.set(Integer.parseInt(itr.nextToken()));
      if(param.equals("false"))
      {
        if(!word.toString().equals("World"))
          context.write(word, one);

      }

      else{
          context.write(word, one);
      }
      
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("wor", args[1]);

    Job job = Job.getInstance(conf, "Covid19_1");
    job.setJarByClass(Covid19_1.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    // Path OutputPath = new Path(args[1]);
    // OutputPath.getFileSystem(conf).delete(OutputPath, true);

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}