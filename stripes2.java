import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class stripes2 
{
    public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {

        //private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();


        private MapWritable myMap = new MapWritable();

        private int window = 5;

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            /* stemming words */

        	String[] tokens = value.toString().split("[^\\w']+");

            for (int i=0; i<tokens.length; i++) {

                String t = tokens[i];

                myMap.clear();

                for(int j=Math.max(0, i-window); j<Math.min(tokens.length, i+window); j++){
                    if(j == i){
                        continue;
                    }

                    if(tokens[j].length() == 0){
                        continue;
                    }

                    if(myMap.containsKey(new Text(tokens[j]))){
                        IntWritable temp = (IntWritable)myMap.get(new Text(tokens[j]));
                        int x = temp.get();
                        x++;
                        temp.set(x);
                        myMap.put(new Text(tokens[j]), temp);
                    } else {
                        myMap.put(new Text(tokens[j]), new IntWritable(1));
                    }

                }

                word.set(t);
                context.write(word, myMap);

            }
        }
    }

    public static class Reduce extends Reducer<Text, MapWritable, Text, MapWritable> {

        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context)
                throws IOException, InterruptedException {

            //making associative array
            MapWritable ret = new MapWritable();

            for (MapWritable value : values){

                for(MapWritable.Entry<Writable, Writable> e : value.entrySet()){
                    if (ret.containsKey(e.getKey())) {
                        int i = ((IntWritable)e.getValue()).get();
                        int j = ((IntWritable)ret.get(e.getKey())).get();
                        ret.put(e.getKey(), new IntWritable(i+j));
                    } else {
                        ret.put(e.getKey(), e.getValue());
                    }
                }

            }

            context.write(key, ret);
        }
    }

 
    public static void main(String[] args) throws Exception {
    	
    	if (args.length < 2) {
            System.err.println("Must pass InputPath and OutputPath.");
            System.exit(1);
        }
    	
        Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Stripes");
        job.setJarByClass(stripes2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        job.setMapperClass(Map.class);
        //job.setCombinerClass(Reduce.class); // enable this to use 'local aggregation'
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
