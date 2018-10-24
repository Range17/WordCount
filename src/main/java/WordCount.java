import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;


import java.io.IOException;



public class WordCount {
        /**
         * 四个泛型类型分别代表：
         * KeyIn        Mapper的输入数据的Key，这里是每行文字的起始位置（0,11,...）
         * ValueIn      Mapper的输入数据的Value，这里是每行文字
         * KeyOut       Mapper的输出数据的Key，这里是每行文字中的“年份”
         * ValueOut     Mapper的输出数据的Value，这里是每行文字中的“气温”
         */



//        map的作用是将字符切割，获取我们需要的字符
//        Writable接口是用来序列化的，hadoop中的键和值（key-value）是必须实现了Writable接口的对象，输入输出流接口的感觉
        static class TempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
            @Override
//            Text是utf-8的Writable类，Context记录了map执行的上下文
            public void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {
                // 打印样本: Before Mapper: 0, 2000010115
                System.out.print("Before Mapper: " + key + ", " + value);
                String line = value.toString();//将其转换为字符串
                String year = line.substring(0, 4);//返回字符的子字符串，年份
                int temperature = Integer.parseInt(line.substring(8));

//                到此已经分割了，剩下年份和温度，建立处理结束的键值对
                context.write(new Text(year), new IntWritable(temperature));
                // 打印样本: After Mapper:2000, 15
                System.out.println(
                        "======" +
                                "After Mapper:" + new Text(year) + ", " + new IntWritable(temperature));
            }
        }

        /**
         * 四个泛型类型分别代表：
         * KeyIn        Reducer的输入数据的Key，这里是每行文字中的“年份”
         * ValueIn      Reducer的输入数据的Value，这里是每行文字中的“气温”
         * KeyOut       Reducer的输出数据的Key，这里是不重复的“年份”
         * ValueOut     Reducer的输出数据的Value，这里是这一年中的“最高气温”
         */
//        将被切割的字符汇总，然后进行排序/other
        static class TempReducer extends
                Reducer<Text, IntWritable, Text, IntWritable> {
            @Override
            public void reduce(Text key, Iterable<IntWritable> values,
                               Context context) throws IOException, InterruptedException {
                int maxValue = Integer.MIN_VALUE;
                StringBuffer sb = new StringBuffer();
                //读取处理完的数据，取values的最大值
                for (IntWritable value : values) {
                    maxValue = Math.max(maxValue, value.get());
                    sb.append(value).append(", ");
                }
                // 打印样本： Before Reduce: 2000, 15, 23, 99, 12, 22,
                System.out.print("Before Reduce: " + key + ", " + sb.toString());

//                所以这个context的作用是重新建立了键值对
                context.write(key, new IntWritable(maxValue));
                // 打印样本： After Reduce: 2000, 99
                System.out.println(
                        "======" +
                                "After Reduce: " + key + ", " + maxValue);
            }
        }

        public static void main(String[] args) throws Exception {

//            所以如果单机版本的hadoop,只引入依赖，那么路径一定要hdfs吗

            //输入路径
            String dst = "hdfs://localhost:9000/intput.txt";
//            String dst = "D:\\input.txt";
            //输出路径，必须是不存在的，空文件加也不行。
            String dstOut = "hdfs://localhost:9000/output";
//            String dstOut = "D:\\ontput";

//            ？？作用
            Configuration hadoopConfig = new Configuration();

            hadoopConfig.set("fs.hdfs.impl",
                    org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
            );
            hadoopConfig.set("fs.file.impl",
                    org.apache.hadoop.fs.LocalFileSystem.class.getName()
            );
            Job job = new Job(hadoopConfig);

            //如果需要打成jar运行，需要下面这句
            //job.setJarByClass(NewMaxTemperature.class);

            //job执行作业时输入和输出文件的路径

            FileInputFormat.addInputPath(job, new Path(dst));
            FileOutputFormat.setOutputPath(job, new Path(dstOut));

            //指定自定义的Mapper和Reducer作为两个阶段的任务处理类
            job.setMapperClass(TempMapper.class);
            job.setReducerClass(TempReducer.class);

            //设置最后输出结果的Key和Value的类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //执行job，直到完成
            job.waitForCompletion(true);
            System.out.println("Finished");
        }
    }
//
//
//
//
//
//
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//
//import java.io.IOException;
//import java.util.StringTokenizer;
//
//public class WordCount {
//
//    public static class TokenizerMapper
//            extends Mapper<Object, Text, Text, IntWritable> {
//
//        private final static IntWritable one = new IntWritable(1);
//        private Text word = new Text();
//
//        public void map(Object key, Text value, Mapper.Context context
//        ) throws IOException, InterruptedException {
//            StringTokenizer itr = new StringTokenizer(value.toString());
//            while (itr.hasMoreTokens()) {
//                word.set(itr.nextToken());
//                context.write(word, one);
//            }
//        }
//    }
//
//    public static class IntSumReducer
//            extends Reducer<Text, IntWritable, Text, IntWritable> {
//        private IntWritable result = new IntWritable();
//
//        public void reduce(Text key, Iterable<IntWritable> values,
//                           Context context
//        ) throws IOException, InterruptedException {
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//            result.set(sum);
//            context.write(key, result);
//        }
//    }
//
//    public static void main(String[] args) throws Exception {
//        Configuration conf = new Configuration();
//        Job job = Job.getInstance(conf, "word count");
//        job.setJarByClass(WordCount.class);
//        job.setMapperClass(TokenizerMapper.class);
//        job.setCombinerClass(IntSumReducer.class);
//        job.setReducerClass(IntSumReducer.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
//}
