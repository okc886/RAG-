package net.topikachu.rag.chat.history;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.chat.history.entity.ChatMemorySnapshotEntity;
import net.topikachu.rag.chat.history.mapper.ChatMemorySnapshotMapper;
import org.springframework.ai.chat.memory.ChatMemoryRepository;
import org.springframework.ai.chat.messages.Message;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.List;

/**
 * SpringAI ChatMemoryRepository 实现类
 * MySQL+Redis二级缓存存储对话会话快照
 * MySQL：持久化存储完整会话二进制快照
 * Redis：热点缓存，TTL7天，加速会话读取；Redis异常不阻断主业务
 * 存储模型：一个会话ID对应数据库一行记录，存储全部消息二进制blob，快照覆盖模式，不做单条消息增量
 */
@Slf4j
@Repository
// 条件装配：rag.chat.memory.serializer=legacy 时生效；不配置该属性默认启用
@ConditionalOnProperty(prefix = "rag.chat.memory", name = "serializer", havingValue = "legacy", matchIfMissing = true)
public class MySqlRedisChatMemoryRepository implements ChatMemoryRepository {

    // Redis key前缀，拼接会话id形成完整key：chat‑memory:snapshot:{conversationId}
    private static final String KEY_PREFIX = "chat-memory:snapshot:";

    // MyBatisPlus Mapper，操作mysql表 chat_memory_snapshot 会话快照表
    private final ChatMemorySnapshotMapper snapshotMapper;
    // RedisTemplate value使用byte[]，直接存储序列化后的二进制字节，避免字符串编解码
    private final RedisTemplate<String, byte[]> chatMemoryRedisTemplate;
    // 消息序列化器：List<Message> <--> byte[] 二进制互转
    private final ChatMessageSerializer serializer;
    // Redis缓存过期时间：7天，冷会话自动淘汰出Redis，访问时回源MySQL重建缓存
    private final Duration cacheTtl;

    public MySqlRedisChatMemoryRepository(ChatMemorySnapshotMapper snapshotMapper,
                                          RedisTemplate<String, byte[]> chatMemoryRedisTemplate,
                                          ChatMessageSerializer serializer) {
        this.snapshotMapper = snapshotMapper;
        this.chatMemoryRedisTemplate = chatMemoryRedisTemplate;
        this.serializer = serializer;
        this.cacheTtl = Duration.ofDays(7);
    }

    /**
     * 查询数据库中全部会话id列表
     * 只查询conversation_id字段，不查询大blob，减少网络IO
     * 注意：无分页，会话数量很大会存在性能问题
     * @return 会话id集合
     */
    @Override
    public List<String> findConversationIds() {
        return snapshotMapper.selectList(Wrappers.<ChatMemorySnapshotEntity>lambdaQuery()
                        .select(ChatMemorySnapshotEntity::getConversationId))
                .stream()
                .map(ChatMemorySnapshotEntity::getConversationId)
                .toList();
    }

    /**
     * 根据会话id读取全部对话消息，二级缓存逻辑：优先Redis，未命中查MySQL，回写Redis缓存
     * @param conversationId 会话唯一ID
     * @return 该会话全部Message消息列表；无数据/反序列化异常返回空集合
     */
    @Override
    public List<Message> findByConversationId(String conversationId) {
        // 1.优先读取Redis缓存
        byte[] cached = chatMemoryRedisTemplate.opsForValue().get(redisKey(conversationId));
        if (cached != null && cached.length > 0) {
            // Redis命中，直接反序列化返回消息
            return deserialize(conversationId, cached);
        }

        // 2.Redis未命中，回源MySQL查询会话快照行
        ChatMemorySnapshotEntity snapshot = snapshotMapper.selectById(conversationId);
        if (snapshot == null || snapshot.getMessageBlob() == null) {
            // 会话不存在或者blob为空，返回空列表
            return List.of();
        }

        // 3.Mysql读取到二进制字节数组，反序列化为消息对象列表
        List<Message> messages = deserialize(conversationId, snapshot.getMessageBlob());
        // 4.回填Redis缓存，后续请求走缓存
        refreshCache(conversationId, snapshot.getMessageBlob());
        return messages;
    }

    /**
     * 保存会话全部消息；快照覆盖模式，传入完整消息列表覆盖旧数据
     * 先序列化消息为二进制，操作MySQL insert/update，成功后刷新Redis缓存
     * 注意：selectById + insert/update 非原子，高并发同会话会主键冲突
     * @param conversationId 会话id
     * @param messages 当前会话完整消息列表
     */
    @Override
    @Transactional
    public void saveAll(String conversationId, List<Message> messages) {
        // 将消息列表序列化为二进制字节数组
        byte[] bytes = serializer.serializeMessages(messages);

        ChatMemorySnapshotEntity entity = new ChatMemorySnapshotEntity();
        entity.setConversationId(conversationId);
        entity.setMessageBlob(bytes); // 完整会话二进制快照
        entity.setMessageCount(messages == null ? 0 : messages.size()); // 当前会话消息条数
        entity.setSerializer(ChatMessageSerializer.SERIALIZER_NAME); // 标记使用的序列化器名称

        // 查询判断会话是否存在，存在更新，不存在插入
        if (snapshotMapper.selectById(conversationId) == null) {
            snapshotMapper.insert(entity);
        } else {
            snapshotMapper.updateById(entity);
        }
        // MySQL落库成功，刷新Redis缓存
        refreshCache(conversationId, bytes);
    }

    /**
     * 删除指定会话：删除MySQL行 + 删除Redis缓存key
     * @param conversationId 会话id
     */
    @Override
    @Transactional
    public void deleteByConversationId(String conversationId) {
        // 删除mysql会话快照记录
        snapshotMapper.deleteById(conversationId);
        // 删除redis缓存；存在风险：mysql删除成功，redis删除失败，残留脏缓存
        chatMemoryRedisTemplate.delete(redisKey(conversationId));
    }

    /**
     * 二进制字节数组反序列化为消息列表，捕获异常，异常返回空列表，避免业务崩溃
     * @param conversationId 会话id，用于日志打印
     * @param bytes 序列化后的二进制数据
     * @return 消息列表，异常返回空List
     */
    private List<Message> deserialize(String conversationId, byte[] bytes) {
        try {
            return serializer.deserializeMessages(bytes);
        } catch (RuntimeException ex) {
            log.warn("Failed to deserialize chat memory snapshot. conversationId={}", conversationId, ex);
            return List.of();
        }
    }

    /**
     * 刷新Redis缓存，写入二进制字节，设置7天TTL；写Redis异常仅打警告，不抛异常，保证主业务可用
     * @param conversationId 会话id
     * @param bytes 需要存入redis的二进制快照
     */
    private void refreshCache(String conversationId, byte[] bytes) {
        try {
            chatMemoryRedisTemplate.opsForValue().set(redisKey(conversationId), bytes, cacheTtl);
        } catch (RuntimeException ex) {
            log.warn("Failed to refresh chat memory Redis cache. conversationId={}", conversationId, ex);
        }
    }

    /**
     * 拼接完整Redis key
     * @param conversationId 会话id
     * @return redis key字符串
     */
    private String redisKey(String conversationId) {
        return KEY_PREFIX + conversationId;
    }
}
