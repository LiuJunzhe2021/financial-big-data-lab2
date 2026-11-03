import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.math.BigDecimal;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Task1{
    public static class InstitutionMapper extends Mapper<LongWritable,Text,Text,Text>{
       @Override 
       protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException{
        String line=value.toString().replace("\uFEFF","").trim();
        if(line.isEmpty() || line.startsWith("CUST_ID,")){
            return;
        }
        String[] fields=line.split(",",-1);
        if(fields.length!=15){
            throw new IOException("文件格式不符合要求"+key);
    
        }
        String org=fields[1].trim();
        String label=fields[2].trim();
        if(!label.equals("0") && !label.equals("1")){
            throw new IOException("无法识别的标签：" + label);
        }
        BigDecimal assets=parseAmount(fields[10]);
        BigDecimal debts=parseAmount(fields[11]);
        int overdue=label.equals("0") ? 1 : 0;
        String stats="1,"+assets.toPlainString()+","+debts.toPlainString()+","+overdue;
        context.write(new Text(org),new Text(stats));}
        private static BigDecimal parseAmount(String text){
            String amount=text.trim();
            if(amount.isEmpty()||amount.equalsIgnoreCase("null")||amount.equalsIgnoreCase("NAN")){
                 return BigDecimal.ZERO;
            }
            return new BigDecimal(amount);

       }
    }
 public static class InstitutionReducer
            extends Reducer<Text, Text, Text, Text> {
               @Override 
               protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
                long customerCount=0;
                long overdueCount=0;
                BigDecimal totalAssets=BigDecimal.ZERO;
                BigDecimal totalDebts=BigDecimal.ZERO;
                for(Text value:values){
                    String[] stats=value.toString().split(",",-1);
                    customerCount+=Long.parseLong(stats[0]);
                    totalAssets=totalAssets.add(new BigDecimal(stats[1]));
                    totalDebts=totalDebts.add(new BigDecimal(stats[2]));
                    overdueCount+=Long.parseLong(stats[3]);

                }
                String result=customerCount+","+totalAssets.toPlainString()+","+totalDebts.toPlainString()+","+overdueCount;
                context.write(key,new Text(result));
               }
            }

        public static void main(String[] args) throws Exception {

    if (args.length != 2) {
        System.err.println(
                "用法：hadoop jar task1.jar Task1 <输入路径> <输出目录>");
        System.exit(2);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "task1-institution-stats");

    // 指定当前程序所在的 JAR。
    job.setJarByClass(Task1.class);

    // 指定负责处理数据的类。
    job.setMapperClass(InstitutionMapper.class);
    job.setCombinerClass(InstitutionReducer.class);
    job.setReducerClass(InstitutionReducer.class);

    // Mapper 输出的 key、value 类型。
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
        