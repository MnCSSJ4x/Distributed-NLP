import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.security.KeyStore.Entry;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;

import opennlp.tools.cmdline.postag.POSModelLoader;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.stemmer.PorterStemmer;

public class term2 {
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, MapWritable> {
		
		private Set<String> stopWords = new HashSet<>();
		PorterStemmer stemmer = new PorterStemmer();
		
		 public void setup(Context context) throws IOException, InterruptedException {
		        // Load stopwords from file
		        URI[] cacheFiles = context.getCacheFiles();
		        if (cacheFiles != null && cacheFiles.length > 0) {
		          try (BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()))) {
		            String line;
		            while ((line = reader.readLine()) != null) {
		              stopWords.add(line.trim());
		            }
		          }
		        }
		      }
	

		
	    

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
			MapWritable tfMap = new MapWritable();
//		    Map<String, Integer> tfMap = new HashMap<>();
		  
			
			if (line != null) {
				SimpleTokenizer tokenizer = SimpleTokenizer.INSTANCE;
		    	String tokenizedLine[] = tokenizer.tokenize(line); //Tokenize line
		    	
		     
		    	
		    	
		    	for(String s: tokenizedLine) {
		    		 if (!stopWords.contains(s)){
		    			 String word = stemmer.stem(s.toLowerCase());
		    			 if(tfMap.containsKey(new Text(word))) {
				    			IntWritable temp =(IntWritable) tfMap.get(new Text(word));
				    			int x = temp.get();
				    			x++;
				    			temp.set(x);
				    			tfMap.put(new Text(word), temp);
				    		}
				    		else {
				    			tfMap.put(new Text(word), new IntWritable(1));
				    		}

		    		 }
		    	}
		    	
	            
		    	
		    	context.write(new Text(fileName), tfMap);
		    		
		    	
			}
			
			
		    
		}
	}

	public static class IntSumReducer extends Reducer<Text, MapWritable, Text, DoubleWritable> {
		private Map<String, Integer> dfMap = new HashMap<>();

	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	        URI[] files = context.getCacheFiles();
	        Path dfPath = new Path(files[1].getPath());
	        try (BufferedReader reader = new BufferedReader(new FileReader(dfPath.toString()))) {
	          String line;
	          while ((line = reader.readLine()) != null) {
	            String[] parts = line.split("\t");
	            String term = parts[0];
	            int df = Integer.parseInt(parts[1]);
	            dfMap.put(term, df);
	            
	          }
	          
	         
		        
	        }
	        
	        
	     
	        
	        

	        
	        
	      }
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<MapWritable> values, Context context)
				throws IOException, InterruptedException {
			MapWritable ret = new MapWritable();
			
			
			
            for (MapWritable value : values){

                for(MapWritable.Entry<Writable, Writable> e : value.entrySet()){
                   		
                		if (ret.containsKey(e.getKey())){                	                    		
                			int i = ((IntWritable) e.getValue()).get();
                			int j = ((IntWritable) ret.get(e.getKey())).get();
                			ret.put(e.getKey(), new IntWritable(i+j));
                		} else {
                			ret.put(e.getKey(), e.getValue());
                		}
                	}
                }
            
            
            
            
	    	for (MapWritable.Entry<Writable, Writable> entry : ret.entrySet()) {
	    		String term = entry.getKey().toString();
	    		int tf = ((IntWritable) entry.getValue()).get();
	    		
	    		int df = dfMap.getOrDefault(term, 0);
			    double score = tf * Math.log10(10000.0 / (df + 1));
				result.set(score);
				String k = key + "\t" + term;
			    context.write(new Text(k), result);
	    		
	    		
	        }
	
			
		
		}

	}

	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		//Creating Configuration
	    Job job = Job.getInstance(conf, "document frequency");
	    //Add link to Stopwords.txt 
	    job.addCacheFile(new URI("/Users/monjoy/Desktop/Assignment2/stopwords.txt"));
	    job.addCacheFile(new URI("/Users/monjoy/Desktop/Assignment2/out-wiki-test/part-r-00000"));
	    
	    job.setMapperClass(TokenizerMapper.class);
	    job.setReducerClass(IntSumReducer.class);
	    
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(MapWritable.class);
	    //	    job.setOutputFormatClass(OutputFormat.class);
	
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}

}
