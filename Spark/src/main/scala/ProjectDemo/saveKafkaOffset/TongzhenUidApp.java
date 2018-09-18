package ProjectDemo.saveKafkaOffset;

import com.alibaba.fastjson.JSONObject;
import com.bj58.ecdata.sparkstream.app.bean.TongzhenApp;
import com.bj58.ecdata.sparkstream.app.bean.TongzhenCateApp;
import com.bj58.ecdata.sparkstream.app.redis.JedisUtils;
import com.bj58.ecdata.sparkstream.app.utils.KafkaManager;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @Author: wangchonglu
 * @Date: 2018/8/15 14:31
 * @Description:   同镇uid uv+pv
 *
 */
public class TongzhenUidApp {

    private static Logger log = LoggerFactory.getLogger(TongzhenUidApp.class);

    private static  int  batchInterval = 2000; //切片固定2s

    private static final ExecutorService threadPool = Executors.newCachedThreadPool();

    public static final String ERROR = "ERROR";

    private static final String SEPARATOR = "\t";

    private static final String  M_TONGZHEN_TOPIC = "hdp_lbg_ecdata_basestat_tongzhen_m_track_58_com";

    final static AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();



    public static void main(String[] args) throws InterruptedException, SparkException {


        String parallelism = args[0];
        batchInterval = Integer.parseInt(args[1]);
        String groupId = args[2];


        //spark 运行参数
        SparkConf conf = new SparkConf()
                .set("spark.streaming.unpersist", "true") //Spark来计算哪些RDD需要持久化，这样有利于提高GC的表现。
                .set("spark.default.parallelism", parallelism)	//reduceByKeyAndWindow执行时启动的线程数，默认是8个
                .set("spark.yarn.driver.memoryOverhead", "1024") //Driver的堆外内存
                .set("spark.yarn.executor.memoryOverhead", "1024") //Executor的堆外内存
                .set("spark.storage.memoryFraction", "0.5")
//                .setAppName("appstart")
//                .setMaster("local[3]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//                .set("spark.kryo.registrator", "com.bj58.ecdata.spark.application.kryo.Registrator");


        JavaStreamingContext jssc = new JavaStreamingContext(conf,new Duration(batchInterval));



        //kafka topic
        Set<String> topics =new HashSet<String>();
        topics.add(M_TONGZHEN_TOPIC);


        Map<String,String> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "10.126.99.105:9092,10.126.99.196:9092,10.126.81.208:9092,10.126.100.144:9092,10.126.81.215:9092");
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "largest");

        final KafkaManager javaKafkaManager = new KafkaManager(kafkaParams);

        JavaInputDStream<String> lines = javaKafkaManager.createDirectStream(jssc, kafkaParams, topics);



        JavaPairDStream<String, TongzhenApp> mapStreams = lines.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> v1) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) v1.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                return v1;
            }
        }).mapToPair(

                new PairFunction<String, String, TongzhenApp>() {

                    @Override
                    public Tuple2<String, TongzhenApp> call(String content) throws Exception {

                        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmm");
                        String realtime = format.format(new Date()); // 实时时间 到分钟的全部时间
                        String today = realtime.substring(0, 8); //当日日期;
                        int minute = Integer.valueOf(realtime.substring(10, 12)); // 实时分钟
                        String section = realtime.substring(0, 10) + (minute /10) + "0"; // 完整实时区间，处理过的前10分钟的时间


                        try{

                            //日志解析
                            JSONObject logJson = JSONObject.parseObject(content);
                            JSONObject trackUrl = JSONObject.parseObject(logJson.get("trackUrl").toString());

                            String userId = trackUrl.getString("userId") == null?"-":trackUrl.getString("userId");
                            String cookie = logJson.getString("cookieID")== null?"-":logJson.getString("cookieID");
                            TongzhenApp tongzhenApp = new TongzhenApp();

                            tongzhenApp.setUserId(userId);
                            Set<String> cookies = new HashSet<String>();
                            cookies.add(cookie);
                            tongzhenApp.setCookies(cookies);
                            tongzhenApp.setPv(1);
                            tongzhenApp.setToday(today);

                            String weidu = userId + SEPARATOR + section;


                            return new Tuple2<String, TongzhenApp>(weidu, tongzhenApp);

                        }catch (Exception e){
                            log.info(" MrTongzhenInfoUid map ERROR!",e);
                        }

                        return new Tuple2<String, TongzhenApp>(ERROR, new TongzhenApp());
                    }
                });


        JavaPairDStream<String, TongzhenApp> reduceStream = mapStreams.reduceByKey(new Function2<TongzhenApp, TongzhenApp, TongzhenApp>() {
            private static final long serialVersionUID = 6264099295078802382L;
            public TongzhenApp call(TongzhenApp v1, TongzhenApp v2)
                    throws Exception {

                try {

                    if(v1.getToday() == null){
                        return v1;
                    }

                    long pv = v1.getPv() + v2.getPv();

                    Set<String> cookies = new HashSet<String>();
                    cookies.addAll(v1.getCookies());
                    cookies.addAll(v2.getCookies());
                    v1.setCookies(cookies);
                    v1.setPv(pv);
                    return v1;

                } catch (Exception e) {
                    log.error("MrTongzhenCityCateApp reduce ERROR!",e);
                }
                return v1;
            }
        });



        reduceStream.foreachRDD(new VoidFunction<JavaPairRDD<String,TongzhenApp>>() {
            private static final long serialVersionUID = -8199089522425453338L;

            public void call(JavaPairRDD<String, TongzhenApp> v) throws Exception {
                v.foreachPartition(new VoidFunction<Iterator<Tuple2<String,TongzhenApp>>>() {
                    private static final long serialVersionUID = 3040955589442973044L;
                    public void call(Iterator<Tuple2<String, TongzhenApp>> iter)
                            throws Exception {


                        if(null != iter){

                            while(iter.hasNext()){
                                Tuple2<String, TongzhenApp> tuple = iter.next();
                                final String keyStr = tuple._1;
                                final TongzhenApp entity = tuple._2;

                                if(!ERROR.equals(keyStr)){


                                    //结果写入缓存
                                    threadPool.submit(new Runnable() {
                                        @Override
                                        public void run() {

                                            try{

                                                String weidu = "uid_" + keyStr.split(SEPARATOR)[0];
                                                String section = keyStr.split(SEPARATOR)[1];
                                                //pv
                                                JedisUtils.incrBy(2,weidu+"_"+section+"_pv",entity.getPv());
                                                JedisUtils.expire(2,weidu+"_"+section+"_pv",60*60*24*7);
                                                //uv
                                                //uv
                                                if(entity.getCookies().size() > 0){

                                                    Set<String> imeis = entity.getCookies();
                                                    JedisUtils.setComonDayUv(1,2,entity.getToday(),imeis,weidu,"uv",section);

                                                }

                                            }catch (Exception e){
                                                log.info("写入数据失败！",e);
                                            }

                                        }

                                    });


                                }

                            }


                        }
                    }

                });

                //更新kafka偏移量
                javaKafkaManager.updateZKOffsets(offsetRanges.get());

            }
        });

        jssc.start();
        jssc.awaitTermination();


    }

}
