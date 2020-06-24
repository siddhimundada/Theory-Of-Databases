import java.io.IOException;
import java.util.StringTokenizer;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.text.ParseException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Covid19_2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable();
    private Text word = new Text();
    private Text current_date = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      if (value.toString().contains("date") )
                return;
      Configuration conf = context.getConfiguration();
      String start_date = conf.get("start-date");
      String end_date = conf.get("end-date");
      
       
      StringTokenizer itr = new StringTokenizer(value.toString(),",");
      current_date.set(itr.nextToken());
      word.set(itr.nextToken());
      itr.nextToken();
      one.set(Integer.parseInt(itr.nextToken()));
      SimpleDateFormat date1 = new SimpleDateFormat("yyyy-MM-dd");
      
      Date start=null,end=null,current=null,a=null,b=null;
      String a1="2019-02-31";
      String b2="2020-04-08";
      try{
       start=date1.parse(start_date);
       end=date1.parse(end_date);
       current=date1.parse(current_date.toString());
       a=date1.parse("2019-2-31");
       b=date1.parse("2020-04-08");}
      catch(ParseException e) {
        e.printStackTrace();
      }
      if(start.after(b) || start.before(a) || start.after(end) || end.before(a) || end.after(b))
          return;
      if((current.after(start) && current.before(end)) || current.equals(start) || current.equals(end))
      {
        
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
    conf.set("start-date", args[1]);
    conf.set("end-date", args[2]);
    Job job = Job.getInstance(conf, "Covid19_2");
    job.setJarByClass(Covid19_2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[3]));

    // Path OutputPath = new Path(args[1]);
    // OutputPath.getFileSystem(conf).delete(OutputPath, true);

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
}