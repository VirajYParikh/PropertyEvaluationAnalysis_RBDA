  import org.apache.hadoop.fs.Path;
  import org.apache.hadoop.io.IntWritable;
  import org.apache.hadoop.io.Text;
  import org.apache.hadoop.mapreduce.Job;
  import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
  import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

  public class PropertyAnalysisJob {

    public static void main(String[] args) throws Exception {
      if (args.length != 2) {
        System.err.println("Usage: Property Analysis <input path> <output path>");
        System.exit(-1);
      }

      // Configuration conf = new Configuration();
      // conf.set("mapreduce.output.textoutputformat.separator", ",");   
      //Job job = new Job();
      Job job = Job.getInstance();

      job.setJarByClass(PropertyAnalysisJob.class);
      job.setJobName("Property Data");

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));
      
      job.setMapperClass(PropertyMapper.class);
      //job.setCombinerClass(PropertyReducer.class);
      job.setReducerClass(PropertyReducer.class);

      job.setNumReduceTasks(1);

      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
      
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
  }
