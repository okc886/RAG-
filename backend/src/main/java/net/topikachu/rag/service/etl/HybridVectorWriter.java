package net.topikachu.rag.service.etl;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.milvus.v2.service.vector.response.InsertResp;
import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.observability.TracingSupport;
import org.springframework.ai.document.Document;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 混合向量写入器
 * 作用：将文档块通过 TEI 服务生成稠密向量 + 稀疏向量，写入 Milvus 向量数据库
 *
 * 稠密向量（embedding）：语义理解，如 "AI" 和 "人工智能" 能匹配
 * 稀疏向量（sparse_vector）：关键词匹配，如 精确匹配 "决策树"
 * 两者结合 = 混合检索，效果更好
 */
@Component
@Slf4j
public class HybridVectorWriter {

    // ============ 依赖注入 ============

    /** TEI 嵌入客户端：调用 TEI 服务生成向量 */
    private final TeiEmbeddingClient teiEmbeddingClient;

    /** Milvus 写入网关：向 Milvus 向量数据库写入数据 */
    private final MilvusWriteGateway milvusWriteGateway;

    /** 链路追踪支持：记录调用链、耗时、标签等 */
    private final TracingSupport tracingSupport;

    /** Gson：用于 JSON 序列化/反序列化 */
    private final Gson gson = new Gson();

    // ============ 构造函数 ============

    public HybridVectorWriter(TeiEmbeddingClient teiEmbeddingClient,
                              MilvusWriteGateway milvusWriteGateway,
                              TracingSupport tracingSupport) {
        this.teiEmbeddingClient = teiEmbeddingClient;
        this.milvusWriteGateway = milvusWriteGateway;
        this.tracingSupport = tracingSupport;
    }

    // ============ 核心方法：写入 ============

    /**
     * 批量写入文档块到 Milvus
     *
     * @param documents 文档块列表（每个块是一段文本 + 元数据）
     * @return Mono<Void> 完成信号
     *
     * 流程：
     *   1. 遍历每个文档块
     *   2. 调用 TEI 生成稠密向量 + 稀疏向量
     *   3. 构造 Milvus 插入行（doc_id, content, metadata, embedding, sparse_vector）
     *   4. 批量写入 Milvus
     *   5. 单个块失败不影响整体（跳过并记录日志）
     */
    public Mono<Void> write(List<Document> documents) {
        // 如果文档块列表为空，直接返回空 Mono
        if (documents == null || documents.isEmpty()) {
            return Mono.empty();
        }

        log.info("Writing {} documents with hybrid vectors via TEI", documents.size());

        // 统计跳过的块数（TEI 调用失败的块）
        AtomicInteger skippedChunks = new AtomicInteger();

        // 构建链路追踪标签（文档UUID、文件名等）
        Map<String, Object> traceTags = vectorTraceTags(documents);

        // ============ 核心步骤1：为每个子块文档块生成向量 ============
        Mono<List<JsonObject>> embedRows = Flux.fromIterable(documents)
                // flatMap：对每个文档块调用 TEI 嵌入服务
                // concurrency=4：同时最多 4 个并发请求，防止压垮 TEI 服务
                .flatMap(doc -> teiEmbeddingClient.embed(doc.getText())
                                // 处理 TEI 返回的响应
                                .map(response -> {
                                    // ----- 1.1 提取稠密向量（embedding）-----
                                    // 稠密向量：固定长度的浮点数数组，表示语义含义
                                    List<Float> denseVector = Collections.emptyList();
                                    if (response.denseVecs() != null && !response.denseVecs().isEmpty()) {
                                        denseVector = response.denseVecs().get(0);  // 取第一个（每个文档只传了一个文本）
                                    }

                                    // ----- 1.2 提取稀疏向量（sparse_vector）-----
                                    // 稀疏向量：{词编号: 权重} 的字典，表示关键词
                                    // 使用 TreeMap 保持 key 有序
                                    SortedMap<Long, Float> sparseVector = new TreeMap<>();
                                    if (response.sparseVecs() != null && !response.sparseVecs().isEmpty()) {
                                        sparseVector = teiEmbeddingClient.parseSparse(response.sparseVecs().get(0));
                                    }

                                    // ----- 1.3 构造 Milvus 插入行（JsonObject）-----
                                    // Milvus 集合结构：
                                    //   - doc_id: 文档块ID
                                    //   - content: 文本内容
                                    //   - metadata: 元数据（文件名、页数等）
                                    //   - embedding: 稠密向量
                                    //   - sparse_vector: 稀疏向量
                                    JsonObject row = new JsonObject();

                                    // doc_id：如果没有则自动生成 UUID
                                    row.addProperty("doc_id", doc.getId() != null ? doc.getId() : UUID.randomUUID().toString());

                                    // content：文本内容
                                    row.addProperty("content", doc.getText());

                                    // metadata：元数据（转为 JSON 字符串）
                                    row.add("metadata", gson.toJsonTree(doc.getMetadata()));

                                    // embedding：稠密向量（转为 JSON 数组）
                                    row.add("embedding", gson.toJsonTree(denseVector));

                                    // sparse_vector：稀疏向量（转为 JSON 对象）
                                    row.add("sparse_vector", gson.toJsonTree(sparseVector));

                                    return row;
                                })
                                // ----- 1.4 异常处理：单个块失败不影响整体 -----
                                // 如果 TEI 调用失败（网络超时、服务异常等），跳过该块并记录日志
                                .onErrorResume(error -> {
                                    skippedChunks.incrementAndGet();  // 跳过的块数 +1
                                    log.warn(
                                            "Skipping chunk during hybrid vector write: docUuid={}, pageNumber={}, preview={}, reason={}",
                                            doc.getMetadata().getOrDefault("doc_uuid", "unknown"),
                                            doc.getMetadata().getOrDefault("page_number", "unknown"),
                                            TextSanitizer.preview(doc.getText()),  // 预览文本（防止日志太大）
                                            error.getMessage());
                                    return Mono.empty();  // 返回空，跳过这个块
                                }),
                        4)  // concurrency=4：限制并行 TEI 调用
                .collectList();  // 收集所有结果到 List

        // ============ 核心步骤2：写入 Milvus ============
        return tracingSupport.traceMono("etl.embed", traceTags, embedRows)
                .flatMap(rows -> {
                    // ----- 2.1 检查：所有块都失败了 -----
                    // 如果所有块都嵌入失败（如 TEI 服务完全宕机），抛出异常让上层重试
                    if (rows.isEmpty()) {
                        return Mono.error(new IllegalStateException("All document chunks failed embedding"));
                    }

                    // ----- 2.2 批量写入 Milvus -----
                    // 调用 MilvusWriteGateway.insert() 批量插入
                    // 链路追踪标签：doc_uuid 和 chunk_count
                    return tracingSupport.traceMono("etl.vector_upsert",
                            Map.of(
                                    "document.doc_uuid", String.valueOf(traceTags.getOrDefault("document.doc_uuid", "")),
                                    "etl.chunk_count", rows.size()),
                            milvusWriteGateway.insert(rows));
                })
                // ----- 2.3 记录插入结果 -----
                .doOnNext(this::logInsert)  // 打印插入数量
                // ----- 2.4 记录跳过的块数 -----
                .doOnSuccess(v -> {
                    int skipped = skippedChunks.get();
                    if (skipped > 0) {
                        log.warn("Skipped {} chunk(s) during hybrid vector write", skipped);
                    }
                })
                .then();  // 转换为 Mono<Void>（忽略结果，只关心完成信号）
    }

    // ============ 删除方法 ============

    /**
     * 根据文档UUID删除 Milvus 中的向量
     * 用途：文档被删除时，同步清理向量数据，避免孤儿数据
     *
     * @param docUuid 文档UUID
     * @return Mono<Void> 完成信号
     */
    public Mono<Void> deleteByDocUuid(String docUuid) {
        if (docUuid == null || docUuid.isEmpty()) {
            return Mono.empty();
        }

        return milvusWriteGateway.deleteByDocUuid(docUuid)
                .doOnSuccess(ignored -> log.info("Cleaned up vectors for docUuid={}", docUuid))
                // 如果删除失败（如向量不存在），只记录警告，不抛异常
                .onErrorResume(e -> {
                    log.warn("Failed to cleanup vectors for docUuid={}, may not exist", docUuid, e);
                    return Mono.empty();
                });
    }

    // ============ 辅助方法 ============

    /**
     * 记录 Milvus 插入结果日志
     */
    private void logInsert(InsertResp response) {
        log.info("Inserted documents to Milvus, insertCnt: {}", response.getInsertCnt());
    }

    /**
     * 构建链路追踪标签
     * 从第一个文档块的元数据中提取 doc_uuid 和 file_name
     */
    private Map<String, Object> vectorTraceTags(List<Document> documents) {
        Map<String, Object> tags = new LinkedHashMap<>();
        tags.put("etl.chunk_count", documents.size());  // 块数量
        if (!documents.isEmpty()) {
            Map<String, Object> metadata = documents.get(0).getMetadata();
            tags.put("document.doc_uuid", metadata.get("doc_uuid"));     // 文档UUID
            tags.put("document.file_name", metadata.get("file_name"));   // 文件名
        }
        return tags;
    }
}