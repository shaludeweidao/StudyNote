// 注意：一定要存在这个包下面
package ProjectDemo.saveKafkaOffset;

import com.esotericsoftware.minlog.Log;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.mutable.ArrayBuffer;
import scala.util.Either;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author wei
 * @date 10/24/17
 */
public class KafkaManager<T> implements Serializable{

    private scala.collection.immutable.Map<String, String> kafkaParams;
    private KafkaCluster kafkaCluster;

    public KafkaManager(Map<String, String> kafkaParams) {
        //TODO
        this.kafkaParams = toScalaImmutableMap(kafkaParams);
        kafkaCluster = new KafkaCluster(this.kafkaParams);
    }

    public JavaInputDStream<String>  createDirectStream(
            JavaStreamingContext jssc,
            Map<String, String> kafkaParams,
            Set<String> topics) throws SparkException {

        String groupId = kafkaParams.get("group.id");

        // 在zookeeper上读取offsets前先根据实际情况更新offsets
        setOrUpdateOffsets(topics, groupId);

        //从zookeeper上读取offset开始消费message
        //TODO
        scala.collection.immutable.Set<String> immutableTopics = JavaConversions.asScalaSet(topics).toSet();
        Either<ArrayBuffer<Throwable>, scala.collection.immutable.Set<TopicAndPartition>> partitionsE
                = kafkaCluster.getPartitions(immutableTopics);

        if (partitionsE.isLeft()){
            throw new SparkException("get kafka partition failed: ${partitionsE.left.get}");
        }
        Either.RightProjection<ArrayBuffer<Throwable>, scala.collection.immutable.Set<TopicAndPartition>>
                partitions = partitionsE.right();
        Either<ArrayBuffer<Throwable>, scala.collection.immutable.Map<TopicAndPartition, Object>> consumerOffsetsE
                = kafkaCluster.getConsumerOffsets(groupId, partitions.get());

        if (consumerOffsetsE.isLeft()){
            throw new SparkException("get kafka consumer offsets failed: ${consumerOffsetsE.left.get}");
        }
        scala.collection.immutable.Map<TopicAndPartition, Object>
                consumerOffsetsTemp = consumerOffsetsE.right().get();
        Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);

        Map<TopicAndPartition, Long> consumerOffsetsLong = new HashMap<TopicAndPartition, Long>();
        for (TopicAndPartition key: consumerOffsets.keySet()){
            consumerOffsetsLong.put(key, (Long)consumerOffsets.get(key));
        }

        JavaInputDStream<String> message = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                String.class,
                kafkaParams,
                consumerOffsetsLong,
                new Function<MessageAndMetadata<String, String>, String>() {
                    public String call(MessageAndMetadata<String, String> v) throws Exception {
                        return v.message();
                    }
                });

        return message;
    }

    /**
     * 创建数据流前，根据实际消费情况更新消费offsets
     * @param topics
     * @param groupId
     */
    private void setOrUpdateOffsets(Set<String> topics, String groupId) throws SparkException {
        for (String topic: topics){
            boolean hasConsumed = true;
            HashSet<String> topicSet = new HashSet<String>();
            topicSet.add(topic);
            scala.collection.immutable.Set<String> immutableTopic = JavaConversions.asScalaSet(topicSet).toSet();
            Either<ArrayBuffer<Throwable>, scala.collection.immutable.Set<TopicAndPartition>>
                    partitionsE = kafkaCluster.getPartitions(immutableTopic);

            if (partitionsE.isLeft()){
                throw new SparkException("get kafka partition failed: ${partitionsE.left.get}");
            }
            scala.collection.immutable.Set<TopicAndPartition> partitions = partitionsE.right().get();
            Either<ArrayBuffer<Throwable>, scala.collection.immutable.Map<TopicAndPartition, Object>>
                    consumerOffsetsE = kafkaCluster.getConsumerOffsets(groupId, partitions);

            if (consumerOffsetsE.isLeft()){
                hasConsumed = false;
            }

            if (hasConsumed){// 消费过
                /**
                 * 如果streaming程序执行的时候出现kafka.common.OffsetOutOfRangeException，
                 * 说明zk上保存的offsets已经过时了，即kafka的定时清理策略已经将包含该offsets的文件删除。
                 * 针对这种情况，只要判断一下zk上的consumerOffsets和earliestLeaderOffsets的大小，
                 * 如果consumerOffsets比earliestLeaderOffsets还小的话，说明consumerOffsets已过时,
                 * 这时把consumerOffsets更新为earliestLeaderOffsets
                 */
                Either<ArrayBuffer<Throwable>, scala.collection.immutable.Map<TopicAndPartition, KafkaCluster.LeaderOffset>>
                        earliestLeaderOffsetsE = kafkaCluster.getEarliestLeaderOffsets(partitions);
                if (earliestLeaderOffsetsE.isLeft()){
                    throw new SparkException("get earliest leader offsets failed: ${earliestLeaderOffsetsE.left.get}");
                }

                scala.collection.immutable.Map<TopicAndPartition, KafkaCluster.LeaderOffset>
                        earliestLeaderOffsets = earliestLeaderOffsetsE.right().get();
                scala.collection.immutable.Map<TopicAndPartition, Object>
                        consumerOffsets = consumerOffsetsE.right().get();

                // 可能只是存在部分分区consumerOffsets过时，所以只更新过时分区的consumerOffsets为earliestLeaderOffsets
                HashMap<TopicAndPartition, Object> offsets = new HashMap<TopicAndPartition, Object>();
                Map<TopicAndPartition, Object>
                        topicAndPartitionObjectMap = JavaConversions.mapAsJavaMap(consumerOffsets);
                for (TopicAndPartition key: topicAndPartitionObjectMap.keySet()){
                    Long n = (Long) topicAndPartitionObjectMap.get(key);
                    long earliestLeaderOffset = earliestLeaderOffsets.get(key).get().offset();
                    if (n < earliestLeaderOffset){
                        System.out.println("consumer group:"
                                + groupId + ",topic:"
                                + key.topic() + ",partition:" + key.partition()
                                + " offsets已经过时，更新为" + earliestLeaderOffset);
                        offsets.put(key, earliestLeaderOffset);
                    }
                }
                if (!offsets.isEmpty()){
                    //TODO
                    scala.collection.immutable.Map<TopicAndPartition, Object>
                            topicAndPartitionLongMap = toScalaImmutableMap(offsets);
                    kafkaCluster.setConsumerOffsets(groupId, topicAndPartitionLongMap);

                }

            }else{// 没有消费过
                String offsetReset = kafkaParams.get("auto.offset.reset").get().toLowerCase();
                scala.collection.immutable.Map<TopicAndPartition, KafkaCluster.LeaderOffset> leaderOffsets = null;
                if ("smallest".equals(offsetReset)){
                    Either<ArrayBuffer<Throwable>, scala.collection.immutable.Map<TopicAndPartition, KafkaCluster.LeaderOffset>>
                            leaderOffsetsE = kafkaCluster.getEarliestLeaderOffsets(partitions);
                    if (leaderOffsetsE.isLeft()) {
                        throw new SparkException("get earliest leader offsets failed: ${leaderOffsetsE.left.get}");
                    }
                    leaderOffsets = leaderOffsetsE.right().get();
                }else {
                    Either<ArrayBuffer<Throwable>, scala.collection.immutable.Map<TopicAndPartition, KafkaCluster.LeaderOffset>>
                            latestLeaderOffsetsE = kafkaCluster.getLatestLeaderOffsets(partitions);
                    if (latestLeaderOffsetsE.isLeft()){
                        throw new SparkException("get latest leader offsets failed: ${leaderOffsetsE.left.get}");
                    }
                    leaderOffsets = latestLeaderOffsetsE.right().get();
                }
                Map<TopicAndPartition, KafkaCluster.LeaderOffset>
                        topicAndPartitionLeaderOffsetMap = JavaConversions.mapAsJavaMap(leaderOffsets);
                Map<TopicAndPartition, Object> offsets = new HashMap<TopicAndPartition, Object>();
                for (TopicAndPartition key: topicAndPartitionLeaderOffsetMap.keySet()){
                    KafkaCluster.LeaderOffset offset = topicAndPartitionLeaderOffsetMap.get(key);
                    long offset1 = offset.offset();
                    offsets.put(key, offset1);
                }

                //TODO
                scala.collection.immutable.Map<TopicAndPartition, Object>
                        immutableOffsets = toScalaImmutableMap(offsets);
                kafkaCluster.setConsumerOffsets(groupId,immutableOffsets);
            }

        }


    }

    /**
     * 更新zookeeper上的消费offsets
     */
    public void updateZKOffsets(OffsetRange[] offsetRanges){
        String groupId = kafkaParams.get("group.id").get();


        for (OffsetRange offset: offsetRanges){
            TopicAndPartition topicAndPartition = new TopicAndPartition(offset.topic(), offset.partition());

            Map<TopicAndPartition, Object> offsets = new HashMap<TopicAndPartition, Object>();
            offsets.put(topicAndPartition, offset.untilOffset());
            Either<ArrayBuffer<Throwable>, scala.collection.immutable.Map<TopicAndPartition, Object>>
                    o = kafkaCluster.setConsumerOffsets(groupId, toScalaImmutableMap(offsets));
            if (o.isLeft()){
                Log.error("Error updating the offset to Kafka cluster: ${o.left.get}");
            }

        }

    }

    /**
     * java Map convert immutable.Map
     * @param javaMap
     * @param <K>
     * @param <V>
     * @return
     */
    private static <K, V> scala.collection.immutable.Map<K, V> toScalaImmutableMap(Map<K, V> javaMap) {
        final java.util.List<Tuple2<K, V>> list = new java.util.ArrayList(javaMap.size());
        for (final Map.Entry<K, V> entry : javaMap.entrySet()) {
            list.add(Tuple2.apply(entry.getKey(), entry.getValue()));
        }
        final scala.collection.Seq<Tuple2<K, V>> seq = scala.collection.JavaConverters.asScalaBufferConverter(list).asScala().toSeq();
        return (scala.collection.immutable.Map<K, V>) scala.collection.immutable.Map$.MODULE$.apply(seq);
    }
}
