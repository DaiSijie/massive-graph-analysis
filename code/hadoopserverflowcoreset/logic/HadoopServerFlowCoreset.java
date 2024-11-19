package hadoopserverflowcoreset.logic;

import hadoopserverflowcoreset.computation.Mapper1;
import hadoopserverflowcoreset.computation.Mapper2;
import hadoopserverflowcoreset.computation.ServerFlowReducer;
import hadoopserverflowcoreset.computation.MaximumMatchingReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class HadoopServerFlowCoreset {

    // input file settings
    public static final int NUMBER_OF_EDGES = Integer.MAX_VALUE; // the number of edges in the graph

    // tell here how many mappers and reducers you want in step 1
    public static final int NUMBER_OF_MAPPERS_1 = 1;
    public static final int NUMBER_OF_REDUCERS_1 = 2;

    public static int linesPerMapper() {
        int linesPerMapper = NUMBER_OF_EDGES / NUMBER_OF_MAPPERS_1;

        if (NUMBER_OF_EDGES % NUMBER_OF_MAPPERS_1 != 0)
            linesPerMapper++;

        return linesPerMapper;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String inputFolder = args[0];
        String outputFolder = args[1];
        String intermediateFolder = outputFolder +  "_intermediate";

        // run the two rounds
        runRound1(conf, inputFolder, intermediateFolder);
        runRound2(conf, intermediateFolder, outputFolder);
    }

    private static void runRound1(Configuration conf, String inputFolder, String outputFolder) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(conf, "CoresetsComputation");
        job.setJarByClass(HadoopServerFlowCoreset.class);

        job.getConfiguration().setInt("mapreduce.task.timeout", 0); // zero means no timeout

        // step 1: configure the mapping step
        job.setMapperClass(Mapper1.class);
        job.setInputFormatClass(NLineInputFormat.class);
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", linesPerMapper());
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // step 2: configure the reduce step
        job.setReducerClass(ServerFlowReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(NUMBER_OF_REDUCERS_1);

        // step3: set io stuff
        FileInputFormat.addInputPath(job, new Path(inputFolder));
        FileOutputFormat.setOutputPath(job, new Path(outputFolder));

        // step 4: run
        job.waitForCompletion(true);
    }

    private static void runRound2(Configuration conf, String inputFolder, String outputFolder) throws IOException, ClassNotFoundException, InterruptedException {
        Job job2 = Job.getInstance(conf, "CoresetsAggregation");
        job2.setJarByClass(HadoopServerFlowCoreset.class);

        job2.getConfiguration().setInt("mapreduce.task.timeout", 0); // zero means no timeout

        // step 1: configure the mapping step
        job2.setMapperClass(Mapper2.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);

        // step 2: configure the reduce step
        job2.setReducerClass(MaximumMatchingReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setNumReduceTasks(1);

        // step3: set io stuff
        FileInputFormat.addInputPath(job2, new Path(inputFolder));
        FileOutputFormat.setOutputPath(job2, new Path(outputFolder));

        // step 4: run
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

}
