import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import javax.naming.Context;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
public class Task2 {
    public static class AverageMapper extends Mapper<LongWritable,Text,Text,Text>{
        private final Map<String,Long> customerCountMap=new HashMap<>();
        private final Map<String,BigDecimal> assetsTotals=new HashMap<>();
        @Override 
        protected void setup(Context context) throws IOException,InterruptedException{
            try(BufferedReader reader=Files.newBufferedReader(Paths.get("institution_stats.txt"),StandardCharsets.UTF_8)){
                String line;
                while((line=reader.readLine())!=null){
                    if(line.trim().isEmpty()){
                        continue;
                    }
                    String[] parts=line.split("\t",2);
                    String org=parts[0].trim();
                    String[] stats=parts[1].split(",",-1);;
                    long count=Long.parseLong(stats[0]);
                    BigDecimal assets=new BigDecimal(stats[1]);
                    customerCountMap.put(org,count);
                    assetsTotals.put(org,assets);
                }
                }
            }
              @Override 
              protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
                String line=value.toString().replace("\uFEFF","").trim();
                if(line.isEmpty()||line.startsWith("CUST_ID,")){
                    return;
                }
                String[] fields=line.split(",",-1);
                if(fields.length!=15){
                    throw new IOException("文件格式不符合要求"+key);
                }
                String org=fields[1].trim();
                BigDecimal assets=parseAmount(fields[10]);
                Long count=customerCountMap.get(org);
                BigDecimal totalAssets=assetsTotals.get(org);
                if(count==null||totalAssets==null||count<=0){
                    throw new IOException("无法计算平均资产："+org);
                }
                boolean atorAbove=assets.multiply(BigDecimal.valueOf(count)).compareTo(totalAssets)>=0;
                String stats=atorAbove ? "1,0":"0,1";
                context.write(new Text(org),new Text(stats));

                
                
              }
              private static BigDecimal parseAmount(String text) {
    String amount = text.trim();

    if (amount.isEmpty()
            || amount.equalsIgnoreCase("null")
            || amount.equalsIgnoreCase("nan")) {
        return BigDecimal.ZERO;
    }

    return new BigDecimal(amount);
}
        }
    
    public static class AverageReducer extends Reducer<Text,Text,Text,Text>{
          @Override 
          protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            long atorAboveCount=0;
            long belowCount=0;
            for(Text value:values){
                String[] stats=value.toString().split(",",-1);
                atorAboveCount+=Long.parseLong(stats[0]);
                belowCount+=Long.parseLong(stats[1]);

            }
            String result=atorAboveCount+","+belowCount;
            context.write(key,new Text(result));
        
          }
    }
     public static void main(String[] args) throws Exception{
            if(args.length!=3){
                System.err.println("用法：hadoop jar task2.jar Task2 <输入路径> <任务一输出目录><输出目录> ");
                System.exit(2);
            }
            Configuration conf=new Configuration();
            Job job=Job.getInstance(conf,"Task2");
            job.setJarByClass(Task2.class);
            job.setMapperClass(AverageMapper.class);
            job.setReducerClass(AverageReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(1);
            job.addCacheFile(new URI(args[1]+"#institution_stats.txt"));
            FileInputFormat.addInputPath(job,new Path(args[0]));
            FileOutputFormat.setOutputPath(job,new Path(args[2]));
            System.exit(job.waitForCompletion(true)?0:1);
           }
        }

