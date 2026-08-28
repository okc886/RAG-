package net.topikachu.rag.service.chat;

// Milvus Java SDK 相关导入，用来构建向量检索请求对象
import io.milvus.v2.service.vector.request.AnnSearchReq;
import io.milvus.v2.service.vector.request.HybridSearchReq;
import io.milvus.v2.service.vector.request.QueryReq;
import io.milvus.v2.service.vector.request.SearchReq;
import io.milvus.v2.service.vector.request.data.FloatVec;
import io.milvus.v2.service.vector.request.data.SparseFloatVec;
import io.milvus.v2.service.vector.request.ranker.RRFRanker;
// lombok日志注解
import lombok.extern.slf4j.Slf4j;
// 权限、用户上下文、搜索范围相关实体
import net.topikachu.rag.auth.CurrentUserContext;
import net.topikachu.rag.auth.SearchScope;
import net.topikachu.rag.business.document.access.KnowledgeAccessPolicy;
import net.topikachu.rag.service.etl.KnowledgeParentBlockService;
import net.topikachu.rag.service.etl.TeiEmbeddingClient;
// Spring‑AI文档对象，检索结果封装为Document
import org.springframework.ai.document.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
// WebFlux响应式异步返回类型
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * 混合检索服务：RAG召回层核心组件
 * 功能：支持稠密向量dense + 稀疏向量sparse混合检索，使用RRF算法做排名融合；支持纯dense检索模式
 * 特点：ACL权限过滤下沉到Milvus检索层，检索阶段直接过滤无权限数据
 */
@Service // Spring Bean，交给Spring容器管理，可以被其他组件注入调用
@Slf4j   // lombok注解，自动生成log日志对象，用于打印日志
public class HybridSearchService {

    // Milvus集合名称，对应数据库中的表，读取配置文件，默认vector_store
    @Value("${spring.ai.vectorstore.milvus.collection-name:vector_store}")
    private String collectionName;

    // Milvus中稠密向量的字段名称，存储1024维语义向量
    @Value("${rag.retrieval.dense-vector-field:embedding}")
    private String denseVectorField;

    // Milvus中稀疏向量的字段名称，存储BM25风格token‑权重数据
    @Value("${rag.retrieval.sparse-vector-field:sparse_vector}")
    private String sparseVectorField;

    // dense/sparse子查询各自先召回多少候选，配置50条，扩大候选池用于后续RRF融合
    @Value("${rag.retrieval.dense-topk:50}")
    private int topK;

    // RRF算法平滑系数k=60：融合dense、sparse两路排名，不会过度惩罚排名靠后的文档，提升召回多样性
    @Value("${rag.retrieval.rrf-k:60}")
    private int rrfK;

    private final TeiEmbeddingClient teiEmbeddingClient;      // 调用Python TEI服务，生成dense/sparse向量
    private final MilvusSearchGateway milvusSearchGateway;    // Milvus网关，封装和Milvus服务的网络交互
    private final KnowledgeAccessPolicy accessPolicy;         // 知识库访问权限策略，生成Milvus过滤表达式

    // 构造函数注入依赖，Spring自动传入实例
    public HybridSearchService(TeiEmbeddingClient teiEmbeddingClient, MilvusSearchGateway milvusSearchGateway,
                               KnowledgeAccessPolicy accessPolicy) {
        this.teiEmbeddingClient = teiEmbeddingClient;
        this.milvusSearchGateway = milvusSearchGateway;
        this.accessPolicy = accessPolicy;
    }

    /**
     * 重载方法：对外简易入口，默认开启sparse混合检索模式 useSparse=true
     * @param query 用户查询问题
     * @param currentUserContext 当前用户上下文，携带用户身份信息
     * @param searchScope 搜索范围，限定可检索的知识库空间
     * @param topK 最终希望返回多少条检索结果
     * @return Mono包装的Document列表，Document代表知识库chunk片段
     */
    public Mono<List<Document>> hybridSearch(String query, CurrentUserContext currentUserContext,
                                             SearchScope searchScope, int topK) {
        // 调用完整的hybridSearch实现，默认开启稀疏向量
        return hybridSearch(query, currentUserContext, searchScope, topK, true);
    }

    /**
     * 核心检索实现方法
     * @param query 用户查询问题
     * @param currentUserContext 当前用户上下文
     * @param searchScope 搜索范围
     * @param topK 最终返回结果条数
     * @param useSparse true：dense+sparse混合检索RRF融合；false：仅dense纯语义检索
     * @return Mono<List<Document>> 检索得到的知识库片段集合
     */
    public Mono<List<Document>> hybridSearch(String query, CurrentUserContext currentUserContext,
                                             SearchScope searchScope, int topK, boolean useSparse) {
        // 构造Milvus标量过滤表达式：包含schema版本、访问控制权限、知识空间、标签条件
        // ACL下沉：将权限逻辑生成表达式交给Milvus，检索阶段直接过滤无权数据
        String filterExpr = accessPolicy.buildMilvusFilterExpr(
                currentUserContext,
                searchScope,
                KnowledgeParentBlockService.CHUNK_SCHEMA_VERSION);

        // ---------------- 分支1：useSparse=false，纯Dense模式，仅用语义向量检索 ----------------
        if (!useSparse) {
            // 调用TEI服务，只获取dense稠密向量
            return teiEmbeddingClient.embedDense(query)
                    .flatMap(denseVector -> {
                        // 使用SDK建造者模式，构建Milvus普通向量搜索请求对象
                        SearchReq.SearchReqBuilder<?, ?> builder = SearchReq.builder()
                                .collectionName(collectionName)
                                .annsField(denseVectorField)           // 指定搜索字段：embedding，1024维Dense向量
                                .data(Collections.singletonList(new FloatVec(denseVector))) // 用户query转为FloatVec向量对象
                                .outputFields(Arrays.asList("doc_id", "content", "metadata")) // 查询返回的字段
                                .topK(topK); // 直接返回目标数量结果

                        // 如果权限过滤表达式不为null，附加过滤条件
                        if (filterExpr != null) {
                            builder.filter(filterExpr);
                        }
                        // 通过网关发送搜索请求到Milvus
                        return milvusSearchGateway.search(builder.build());
                    })
                    // 发生异常时打印错误日志
                    .doOnError(e -> log.error("Dense search failed. query='{}', searchScope={}", query, searchScope, e))
                    // 捕获底层异常，包装为业务异常向上抛出，给上层提示用户
                    .onErrorMap(e -> new RetrievalException("知识库检索失败，请检查向量服务、Milvus 连接或筛选条件后重试。", e));
        }

        // ---------------- 分支2：useSparse=true，混合检索：Dense + Sparse，RRF融合排序 ----------------
        // 调用TEI服务，一次请求同时拿到dense向量和sparse稀疏向量
        return teiEmbeddingClient.embed(query)
                .map(response -> {
                    // 从TEI返回结果中提取Dense向量：1024维浮点数数组
                    List<Float> denseVector = Collections.emptyList();
                    if (response.denseVecs() != null && !response.denseVecs().isEmpty()) {
                        denseVector = response.denseVecs().get(0);
                    }

                    // 从TEI返回结果中提取Sparse向量：BM25风格的词项tokenId→权重映射，TreeMap有序存储
                    SortedMap<Long, Float> sparseMap = new TreeMap<>();
                    if (response.sparseVecs() != null && !response.sparseVecs().isEmpty()) {
                        sparseMap = teiEmbeddingClient.parseSparse(response.sparseVecs().get(0));
                    }

                    // 构造Dense子查询：语义相似度ANN检索请求
                    AnnSearchReq.AnnSearchReqBuilder<?, ?> denseReqBuilder = AnnSearchReq.builder()
                            .vectorFieldName(denseVectorField)                      // 指定Dense向量字段：embedding
                            .vectors(Collections.singletonList(new FloatVec(denseVector))) // 用户query的稠密向量
                            .topK(this.topK);                                       // dense子查询召回50条候选

                    // 构造Sparse子查询：词频相似度ANN检索请求，等价关键词BM25检索
                    AnnSearchReq.AnnSearchReqBuilder<?, ?> sparseReqBuilder = AnnSearchReq.builder()
                            .vectorFieldName(sparseVectorField)                     // 指定Sparse向量字段：sparse_vector
                            .vectors(Collections.singletonList(new SparseFloatVec(sparseMap))) // 用户query稀疏向量
                            .topK(this.topK);                                       // sparse子查询召回50条候选

                    // 【安全关键点】两路检索全部挂载同一个权限过滤表达式，防止sparse路径泄露无权限文档
                    if (filterExpr != null) {
                        denseReqBuilder.expr(filterExpr);
                        sparseReqBuilder.expr(filterExpr);
                    }

                    // 构造Milvus混合搜索请求 HybridSearchReq：封装两路ANN子查询 + RRF排名器
                    return HybridSearchReq.builder()
                            .collectionName(collectionName)                         // 指定Milvus集合
                            // 将dense、sparse两个子查询放入混合搜索请求
                            .searchRequests(Arrays.asList(denseReqBuilder.build(), sparseReqBuilder.build()))
                            .ranker(new RRFRanker(rrfK))                            // 指定RRF排名器，传入rrf‑k=60，Milvus内部做分数融合
                            .topK(topK)                                             // RRF融合完成之后，最终返回topK条，一般20条
                            .outFields(Arrays.asList("doc_id", "content", "metadata"))  // 指定返回的字段
                            .build();
                })
                // 将组装完成的HybridSearchReq交给网关，调用Milvus混合检索接口
                .flatMap(milvusSearchGateway::hybridSearch)
                // 混合检索异常打印日志
                .doOnError(e -> log.error("Hybrid search failed. query='{}', searchScope={}", query, searchScope, e))
                // 异常包装为业务异常向上抛出
                .onErrorMap(e -> new RetrievalException("知识库检索失败，请检查向量服务、Milvus 连接或筛选条件后重试。", e));
    }

    /**
     * 根据doc_id直接查询文档chunk片段
     * 不走向量相似度检索，执行Milvus标量条件查询QueryReq
     * @param docId 文档块唯一id
     * @return Mono<Document> 查询到的文档，异常返回空Mono，不阻断链路
     */
    public Mono<Document> getByDocId(String docId) {
        QueryReq queryReq = QueryReq.builder()
                .collectionName(collectionName)
                .filter(String.format("doc_id == \"%s\"", docId)) // 根据doc_id作为过滤条件
                .outputFields(Arrays.asList("doc_id", "content", "metadata"))
                .build();
        return milvusSearchGateway.queryByDocId(queryReq)
                // 查询发生异常，打印警告日志，返回Mono.empty()，不抛出异常打断流程
                .onErrorResume(e -> {
                    log.warn("Query by doc_id failed: {}", docId, e);
                    return Mono.empty();
                });
    }

    /**
     * 服务预热方法
     * 发送一条测试query，完整执行一次混合检索；提前加载TEI embedding模型、初始化Milvus连接池
     * 消除第一次真实用户请求的冷启动延迟
     * @return Mono<Void> 异步完成信号
     */
    public Mono<Void> warmup() {
        return hybridSearch("warmup query", null, SearchScope.empty(), 1).then();
    }
}
