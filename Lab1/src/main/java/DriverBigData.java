import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DriverBigData extends Configured implements Tool {


    @Override
    //Specify a JOB task
    public int run(String[] args) throws Exception {

        int numberOfReducers;
        // create a job task object
        Job job = Job.getInstance(this.getConf(), "Basic MapReduce Project-WorldCount Example");
        job.setJarByClass(DriverBigData.class);
        // configured this job object
        // first step specify file read method and  read Path
        job.setInputFormatClass(TextInputFormat.class);
        numberOfReducers= Integer.parseInt(args[0]);
        TextInputFormat.addInputPath(job, new Path(args[1]));
        //second step specify how Map phase is processed
        job.setMapperClass(MapperBigData.class);
        //set the  data type of k2,v2 in map phase
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //第三，第四，第五，第六，分区，排序，规约，合并我们默认处理
        // seventh step  specify how Reduce phase is processed
        job.setReducerClass(ReducerBigdata.class);
        // set data type of k3，v3
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        // the eight phase set type of output
        job.setOutputFormatClass(TextOutputFormat.class);
        // set Path of Output
        TextOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setNumReduceTasks(numberOfReducers);
        // wait for job to finish
        boolean exitCode = job.waitForCompletion(true);
        return exitCode ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set("mapreduce.framework.name","local");
        configuration.set("yarn.resourcemanager.hostname","local");
        int run = ToolRunner.run( configuration, new DriverBigData(), args);
        System.exit(run);

    }
}
