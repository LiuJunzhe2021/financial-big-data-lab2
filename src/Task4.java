import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import javax.naming.Context;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Task4{
    public static class AssetLevelMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
    private BigDecimal Q25;
    private BigDecimal Q50;
    private BigDecimal Q75;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        Q25 = new BigDecimal(context.getConfiguration().get("Q25"));
        Q50 = new BigDecimal(context.getConfiguration().get("Q50"));
        Q75 = new BigDecimal(context.getConfiguration().get("Q75"));
    }
        private int getLevel(BigDecimal assets){
            if(assets.compareTo(Q25)<=0)
                return 1;
            else if(assets.compareTo(Q50)<=0)
                return 2;
            else if(assets.compareTo(Q75)<=0)
                return 3;
            else
                return 4;
        }
        @Override 
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            String line=value.toString().replace("\uFEFF", "").trim();
            if(line.isEmpty()|| line.startsWith("CUST_ID,")){
                return;
            }
            String[] fields=line.split(",",-1);
            if(fields.length!=15){
                throw new IOException("字段格式不符合要求"+key);
            }
            BigDecimal totalAssets=parseAmount(fields[10]);
            int level=getLevel(totalAssets);
            StringBuilder amounts=new StringBuilder();
            for(int i=3;i<=10;i++){
                if(i>3){
                    amounts.append(",");
                }
                amounts.append(parseAmount(fields[i]).toPlainString());
            }
            context.write(new IntWritable(level), new Text(amounts.toString()));
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
    public static class AssetLevelReducer extends Reducer<IntWritable, Text, Text, Text> {
       private static final String[] LEVEL_NAMES={"低资产", "中资产", "较高资产", "高资产"};
       private static final String[] ASSET_NAMES={ "存款", "理财", "私人理财", "基金",
            "国债", "银保通", "贵金属"
    };
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        BigDecimal[] sum=new BigDecimal[8];
        Arrays.fill(sum,BigDecimal.ZERO);
        for(Text value:values){
            String[] amounts=value.toString().split(",",-1);
            if(amounts.length!=8){
                throw new IOException("字段格式不符合要求"+key);
            }
            for(int i=0;i<8;i++){
                sum[i]=sum[i].add(new BigDecimal(amounts[i]));
            }

        }
        BigDecimal total=sum[7];
        Integer[] order={0,1,2,3,4,5,6};
        Arrays.sort(order, (a,b)->sum[b].compareTo(sum[a]));
        String levelName=LEVEL_NAMES[key.get()-1];
        for(int index:order){
            BigDecimal ratio=BigDecimal.ZERO.setScale(4);
            if(total.compareTo(BigDecimal.ZERO)!=0){
                ratio=sum[index].divide(total,4,RoundingMode.HALF_UP);
        }
        Text outputKey = new Text(levelName + "\t" + ASSET_NAMES[index]);
Text outputValue = new Text(ratio.toPlainString());

context.write(outputKey, outputValue);
    }
    }
}
   public static void main(String[] args) throws Exception {

    if (args.length != 3) {
        System.err.println(
                "用法：hadoop jar task4.jar Task4 <客户输入> <任务3分位数文件> <输出目录>");
        System.exit(2);
    }

    Configuration conf = new Configuration();
    Path quartilePath = new Path(args[1]);

    // 读取 Task3 输出的那一行分位数。
    try (BufferedReader reader = new BufferedReader(
            new InputStreamReader(
                    quartilePath.getFileSystem(conf).open(quartilePath),
                    StandardCharsets.UTF_8))) {

        String line = reader.readLine();

        if (line == null || line.trim().isEmpty()) {
            throw new IOException("分位数文件为空");
        }

        String[] items = line.trim().split("\t", -1);
        String[] names = {"Q25", "Q50", "Q75"};

        if (items.length != 3) {
            throw new IOException("分位数文件应包含三项");
        }

        for (int i = 0; i < 3; i++) {
            String[] pair = items[i].split(":", 2);

            if (pair.length != 2 || !names[i].equals(pair[0].trim())) {
                throw new IOException("分位数格式错误：" + items[i]);
            }

            conf.set(names[i],
                    new BigDecimal(pair[1].trim()).toPlainString());
        }
    }

    Job job = Job.getInstance(conf, "task4-asset-structure");

    job.setJarByClass(Task4.class);
    job.setMapperClass(AssetLevelMapper.class);
    job.setReducerClass(AssetLevelReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // 按等级编号依次处理四组数据。
    job.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
    }
