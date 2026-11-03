import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Task3 {

    public static class AssetMapper
            extends Mapper<LongWritable, Text, DoubleWritable, IntWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().replace("\uFEFF", "").trim();

            if (line.isEmpty() || line.startsWith("CUST_ID,")) {
                return;
            }

            String[] fields = line.split(",", -1);

            if (fields.length != 15) {
                throw new IOException("字段数量不是15，文件偏移：" + key);
            }

            double assets = parseAmount(fields[10]);

            context.write(new DoubleWritable(assets), new IntWritable(1));
        }

        private static double parseAmount(String text) {
            String amount = text.trim();

            if (amount.isEmpty()
                    || amount.equalsIgnoreCase("null")
                    || amount.equalsIgnoreCase("nan")) {
                return 0.0;
            }

            return Double.parseDouble(amount);
        }
    }

    public static class QuartileReducer
            extends Reducer<DoubleWritable, IntWritable, Text, Text> {
        private long totalCount=0;
        private long cumulativeCount=0;
        private long firstQuartileCount;
        private long secondQuartileCount;
        private long thirdQuartileCount;
        private Double Q25=null;
        private Double Q50=null;
        private Double Q75=null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            totalCount=context.getConfiguration().getLong("quantile.total.count",0L);
            if (totalCount<=0){
                throw new IOException("总客户数必须大于0");

            }
            firstQuartileCount=(long)Math.ceil(totalCount*0.25);
            secondQuartileCount=(long)Math.ceil(totalCount*0.5);
            thirdQuartileCount=(long)Math.ceil(totalCount*0.75);
        }
        @Override 
        protected void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
           long currentCount=0;
           for(IntWritable value:values){
            currentCount+=value.get();
           }
               cumulativeCount+=currentCount;

               if(Q25==null && cumulativeCount>=firstQuartileCount){
                Q25=key.get();
               }
               if(Q50==null && cumulativeCount>=secondQuartileCount){
                Q50=key.get();
               }
               if(Q75==null && cumulativeCount>=thirdQuartileCount){
                Q75=key.get();
               }
        }
        @Override 
        protected void cleanup(Context context) throws IOException, InterruptedException{
            if(cumulativeCount!=totalCount){
                throw new IOException("累计人数和预期不一致");
            }
            if(Q25==null || Q50==null || Q75==null){
                throw new IOException("四分位数计算失败");
            }
            context.write(new Text("Q25:"+Q25),new Text("Q50:"+Q50+"\tQ75:"+Q75));

        }
    }

    public static void main(String[] args) throws Exception {
           if (args.length != 3) {
            System.err.println("Usage: Task3 <用户输入> <任务1统计文件> <输出目录>");
            System.exit(2);

    }
    Configuration conf = new Configuration();;
     Path statsPath = new Path(args[1]);
     long totalCount=0;
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(statsPath.getFileSystem(conf).open(statsPath), StandardCharsets.UTF_8))){

            String line;
            while((line=reader.readLine())!=null){
                if(line.trim().isEmpty()){
                    continue;
                }
                String[] parts=line.split("\t",2);
                String[] stats=parts[1].split(",",-1);
                totalCount+=Long.parseLong(stats[0]);
            }
        }
        if (totalCount<=0){
            throw new IOException("客户总数不能小于等于0");
        }
        conf.setLong("quantile.total.count", totalCount);

    Job job = Job.getInstance(conf, "task3-asset-quartiles");

    job.setJarByClass(Task3.class);
    job.setMapperClass(AssetMapper.class);
    job.setReducerClass(QuartileReducer.class);

    job.setMapOutputKeyClass(DoubleWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
