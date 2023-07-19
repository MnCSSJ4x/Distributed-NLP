import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;

public class Stripes {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, MapWritable> {

	
		private Text word = new Text();
		private static POSModel model = new POSModelLoader().load(new File("/Users/monjoy/Desktop/Assignment2/opennlp-en-ud-ewt-pos-1.0-1.9.3.bin"));
	    private static POSTaggerME tagger=new POSTaggerME(model);

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			
//			for (String token : tokens) {
//				word.set(token);
//				context.write(word, one);
//			}
//			this.init();
			
			MapWritable map = new MapWritable();
			if (line != null) {
				SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
		    	String tokenizedLine[] = tokenizer.tokenize(line); //Tokenize line
		    	String[] tags = tagger.tag(tokenizedLine); //Instanciate tags

				//POS Tag
		    	POSSample sample = new POSSample(tokenizedLine, tags); //Identify tags
//		    	System.out.println("\n\n" + sample.toString()); //Print tagged sentence
//				for(String token : sample.getTags()){
//					System.out.println(token); //Print tags of words
//			  	}
		    	List<String> words = Arrays.asList(sample.toString().split(" "));
		    	
		    	for(String w : words){
		    		word.set(w.split("_")[1]);
		    		if(map.containsKey(word)) {
		    			IntWritable temp =(IntWritable)map.get(new Text(word));
		    			int x = temp.get();
		    			x++;
		    			temp.set(x);
		    			map.put(new Text(word), temp);
		    		}
		    		else {
		    			map.put(new Text(word), new IntWritable(1));
		    		}
		    	}
		    	
		    	for (MapWritable.Entry<Writable, Writable> entry : map.entrySet()) {
		    		word.set((Text) entry.getKey());
		    		context.write(word, map);
		        }
		    	
		    	
		    	
			}
		
		}
	}

	public static class IntSumReducer extends Reducer<Text, MapWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		private Text word = new Text();

		@Override
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			MapWritable ret = new MapWritable();
			
			
            for (MapWritable value : values){

                for(MapWritable.Entry<Writable, Writable> e : value.entrySet()){
                	if(e.getKey().equals(key)) {                		
                		if (ret.containsKey(e.getKey())){                	                    		
                			int i = ((IntWritable)e.getValue()).get();
                			int j = ((IntWritable)ret.get(e.getKey())).get();
                			ret.put(e.getKey(), new IntWritable(i+j));
                		} else {
                			ret.put(e.getKey(), e.getValue());
                		}
                	}
                }
            }
            
	    	for (MapWritable.Entry<Writable, Writable> entry : ret.entrySet()) {
	    		word.set((Text) entry.getKey());
	    		context.write(word, (IntWritable)entry.getValue());
	        }
			
//			int sum = 0;
//			for (IntWritable val : values) {
//				sum += val.get();
//			}
//			result.set(sum);
//			context.write(key, result);
		}

	}

	public static void main(String[] args) throws Exception {
		
		

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Pairs");

		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class); // enable to use 'local aggregation'
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MapWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
