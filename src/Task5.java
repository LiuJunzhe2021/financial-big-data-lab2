import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Task5{
    private static final String[] FEATURE_NAMES = {
        "asset_level", "has_debt", "loan_state", "fund_flag"
    };
    private static final int[] FEATURE_SIZES = {4, 2, 3, 2};
    private static BigDecimal parseAmount(String text){
        String amount=text.trim();
        if(amount.isEmpty()
                || amount.equalsIgnoreCase("null")
                || amount.equalsIgnoreCase("nan")) {
            return BigDecimal.ZERO;
        }
        return new BigDecimal(amount);
    }
    private static int[] extractFeatures(
        String[] fields,
        boolean training,
        BigDecimal[] quartiles) {

    int offset = training ? 1 : 0;

    BigDecimal assets = parseAmount(fields[9 + offset]);
    BigDecimal debt = parseAmount(fields[10 + offset]);
    BigDecimal loanBalance = parseAmount(fields[11 + offset]);

    int assetLevel;

    if (assets.compareTo(quartiles[0]) <= 0) {
        assetLevel = 0;
    } else if (assets.compareTo(quartiles[1]) <= 0) {
        assetLevel = 1;
    } else if (assets.compareTo(quartiles[2]) <= 0) {
        assetLevel = 2;
    } else {
        assetLevel = 3;
    }

    int hasDebt = debt.compareTo(BigDecimal.ZERO) > 0 ? 1 : 0;

    int loanFlag =
            "Y".equalsIgnoreCase(fields[12 + offset].trim()) ? 1 : 0;

    // 组合特征：0=无个贷标识；1=有标识且月余额>0；2=有标识且月余额<=0。
    int loanState = loanFlag == 0 ? 0
            : (loanBalance.compareTo(BigDecimal.ZERO) > 0 ? 1 : 2);

    int credFlag =
            "Y".equalsIgnoreCase(fields[13 + offset].trim()) ? 1 : 0;

    return new int[]{assetLevel, hasDebt, loanState, credFlag};
}
public static class TrainMapper
        extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final LongWritable ONE = new LongWritable(1);
    private final BigDecimal[] quartiles = new BigDecimal[3];

    @Override
    protected void setup(Context context) throws IOException {
        String[] names = {"Q25", "Q50", "Q75"};

        for (int i = 0; i < names.length; i++) {
            String value = context.getConfiguration().get(names[i]);

            if (value == null) {
                throw new IOException("缺少分位数配置：" + names[i]);
            }

            quartiles[i] = new BigDecimal(value);
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().replace("\uFEFF", "").trim();

        if (line.isEmpty() || line.startsWith("CUST_ID,")) {
            return;
        }

        String[] fields = line.split(",", -1);

        if (fields.length != 15) {
            throw new IOException("训练数据应有15列，位置：" + key);
        }

        String label = fields[2].trim();

        if (!label.equals("0") && !label.equals("1")) {
            throw new IOException("标签应为0或1：" + label);
        }

        int[] features = extractFeatures(fields, true, quartiles);

        context.write(new Text("C\t" + label), ONE);

        for (int i = 0; i < features.length; i++) {
            String statKey =
                    "F\t" + label + "\t" + FEATURE_NAMES[i] + "\t" + features[i];

            context.write(new Text(statKey), ONE);
        }
    }
}
public static class TrainReducer
        extends Reducer<Text, LongWritable, Text, LongWritable> {

    @Override
    protected void reduce(
            Text key,
            Iterable<LongWritable> values,
            Context context)
            throws IOException, InterruptedException {

        long total = 0;

        for (LongWritable value : values) {
            total += value.get();
        }

        context.write(key, new LongWritable(total));
    }
}
public static class PredictMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final BigDecimal[] quartiles = new BigDecimal[3];
    private final Map<String, Long> model = new HashMap<>();
    private long totalCount;

    @Override
    protected void setup(Context context) throws IOException {
        String[] names = {"Q25", "Q50", "Q75"};

        for (int i = 0; i < names.length; i++) {
            String value = context.getConfiguration().get(names[i]);

            if (value == null) {
                throw new IOException("缺少分位数配置：" + names[i]);
            }

            quartiles[i] = new BigDecimal(value);
        }

        try (BufferedReader reader = Files.newBufferedReader(
                Paths.get("risk_model.txt"),
                StandardCharsets.UTF_8)) {

            String line;

            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                int separator = line.lastIndexOf('\t');

                if (separator < 0) {
                    throw new IOException("模型行格式错误：" + line);
                }

                String statKey = line.substring(0, separator);
                long count = Long.parseLong(
                        line.substring(separator + 1).trim());

                if (count < 0 || model.putIfAbsent(statKey, count) != null) {
                    throw new IOException("模型计数为负数或统计键重复：" + statKey);
                }
            }
        }

        totalCount = model.getOrDefault("C\t0", 0L)
                + model.getOrDefault("C\t1", 0L);

        if (totalCount <= 0) {
            throw new IOException("模型中没有有效的客户计数");
        }

        // 每个客户在每个特征上只取一个值，因此各取值计数之和应等于类别人数。
        for (int label = 0; label < 2; label++) {
            long classCount = model.getOrDefault("C\t" + label, 0L);
            for (int i = 0; i < FEATURE_SIZES.length; i++) {
                long featureTotal = 0;
                for (int value = 0; value < FEATURE_SIZES[i]; value++) {
                    String statKey = "F\t" + label + "\t" + FEATURE_NAMES[i] + "\t" + value;
                    featureTotal += model.getOrDefault(statKey, 0L);
                }
                if (featureTotal != classCount) {
                    throw new IOException("模型特征计数不完整：标签=" + label
                            + "，特征=" + i);
                }
            }
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().replace("\uFEFF", "").trim();
        if (line.isEmpty() || line.startsWith("CUST_ID,")) {
            return;
        }

        String[] fields = line.split(",", -1);
        if (fields.length != 14) {
            throw new IOException("测试数据应有14列，位置：" + key);
        }

        int[] features = extractFeatures(fields, false, quartiles);
        int prediction = 0;
        double bestScore = Double.NEGATIVE_INFINITY;

        for (int label = 0; label < 2; label++) {
            long classCount = model.getOrDefault("C\t" + label, 0L);

            // 拉普拉斯平滑后的类别先验概率；两个标签，所以分母加2。
            double score = Math.log(
                    (classCount + 1.0) / (totalCount + 2.0));

            for (int i = 0; i < features.length; i++) {
                String statKey = "F\t" + label + "\t" + FEATURE_NAMES[i] + "\t" + features[i];
                long featureCount = model.getOrDefault(statKey, 0L);
                double probability = (featureCount + 1.0)
                        / (classCount + FEATURE_SIZES[i]);
                score += Math.log(probability);
            }

            // 得分相同时保留先处理的标签0。
            if (score > bestScore) {
                bestScore = score;
                prediction = label;
            }
        }

        context.write(new Text(fields[0].trim()), new IntWritable(prediction));
        context.getCounter("PredictionLabels", "label_" + prediction).increment(1);
    }
}

/** 读取Task3的HDFS结果，并把分位数传给每个Mapper。 */
private static void configureQuartiles(Configuration conf, Path quartilePath)
        throws IOException {
    String[] names = {"Q25", "Q50", "Q75"};
    BigDecimal previous = null;

    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
            quartilePath.getFileSystem(conf).open(quartilePath),
            StandardCharsets.UTF_8))) {
        String line = reader.readLine();
        if (line == null || line.trim().isEmpty()) {
            throw new IOException("分位数文件为空");
        }

        String[] items = line.replace("\uFEFF", "").trim().split("\t", -1);
        if (items.length != names.length) {
            throw new IOException("分位数文件应包含Q25、Q50、Q75三项");
        }

        for (int i = 0; i < names.length; i++) {
            String[] pair = items[i].split(":", 2);
            if (pair.length != 2 || !names[i].equals(pair[0].trim())) {
                throw new IOException("分位数格式错误：" + items[i]);
            }
            BigDecimal quartile;
            try {
                quartile = new BigDecimal(pair[1].trim());
            } catch (NumberFormatException e) {
                throw new IOException("分位数不是有效金额：" + items[i], e);
            }
            if (previous != null && quartile.compareTo(previous) < 0) {
                throw new IOException("分位数应满足Q25 <= Q50 <= Q75");
            }
            conf.set(names[i], quartile.toPlainString());
            previous = quartile;
        }
    }
}

public static void main(String[] args) throws Exception {
    boolean training = args.length > 0 && "train".equals(args[0]);
    boolean predicting = args.length > 0 && "predict".equals(args[0]);

    if ((!training && !predicting)
            || (training && args.length != 4)
            || (predicting && args.length != 5)) {
        System.err.println("训练：hadoop jar task5.jar Task5 train"
                + " <训练数据> <Task3分位数文件> <模型输出目录>");
        System.err.println("预测：hadoop jar task5.jar Task5 predict"
                + " <测试数据> <Task3分位数文件> <模型文件> <预测输出目录>");
        System.exit(2);
        return;
    }

    Configuration conf = new Configuration();
    configureQuartiles(conf, new Path(args[2]));

    Job job = Job.getInstance(conf, training
            ? "task5-loan-state-nb-train" : "task5-loan-state-nb-predict");
    job.setJarByClass(Task5.class);
    job.setMapOutputKeyClass(Text.class);
    job.setOutputKeyClass(Text.class);

    Path output;
    if (training) {
        job.setMapperClass(TrainMapper.class);
        job.setCombinerClass(TrainReducer.class);
        job.setReducerClass(TrainReducer.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        // 一个模型文件，便于通过分布式缓存分发。
        job.setNumReduceTasks(1);
        output = new Path(args[3]);
    } else {
        job.setMapperClass(PredictMapper.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setNumReduceTasks(0);

        Path modelPath = new Path(args[3]);
        FileSystem modelFs = modelPath.getFileSystem(conf);
        if (!modelFs.getFileStatus(modelPath).isFile()) {
            throw new IOException("模型文件不存在或不是文件：" + modelPath);
        }
        URI modelUri = modelFs.makeQualified(modelPath).toUri();
        // #后的名称是Mapper工作目录中使用的缓存文件别名。
        job.addCacheFile(new URI(modelUri.getScheme(), modelUri.getAuthority(),
                modelUri.getPath(), modelUri.getQuery(), "risk_model.txt"));
        output = new Path(args[4]);
    }

    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, output);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
