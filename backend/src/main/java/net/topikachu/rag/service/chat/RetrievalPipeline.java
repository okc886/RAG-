package net.topikachu.rag.service.chat;

import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.auth.CurrentUserContext;
import net.topikachu.rag.auth.SearchScope;
import net.topikachu.rag.business.document.entity.KnowledgeParentBlock;
import net.topikachu.rag.observability.TracingSupport;
import net.topikachu.rag.service.etl.KnowledgeParentBlockService;
import org.springframework.ai.document.Document;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * RAG检索流水线：混合检索 + 重排序Rerank + 父块上下文扩展
 * 整体链路：用户query → 混合检索(dense稠密向量+sparse稀疏向量) → Rerank重排打分 → 可选回查父块大上下文
 * WebFlux响应式，全部返回Mono
 */
@Service
@Slf4j
public class RetrievalPipeline {

    private final HybridSearchService hybridSearchService;
    private final RerankService rerankService;
    private final TracingSupport tracingSupport;
    private final KnowledgeParentBlockService parentBlockService;

    /**
     * 构造注入依赖
     * @param hybridSearchService 混合检索服务（稠密+稀疏向量检索RRF融合）
     * @param rerankService 重排序服务，reranker模型对候选块二次打分
     * @param tracingSupport 链路追踪工具，记录各个阶段耗时、标签
     * @param parentBlockService 父块数据库查询服务
     */
    public RetrievalPipeline(HybridSearchService hybridSearchService,
                             RerankService rerankService,
                             TracingSupport tracingSupport,
                             KnowledgeParentBlockService parentBlockService) {
        this.hybridSearchService = hybridSearchService;
        this.rerankService = rerankService;
        this.tracingSupport = tracingSupport;
        this.parentBlockService = parentBlockService;
    }

    /**
     * 对外API：执行检索，只返回子chunk切片Document列表
     * @param query 用户提问
     * @param currentUserContext 当前登录用户上下文，用于Milvus权限过滤
     * @param searchScope 检索范围：知识空间、标签
     * @param hybridTopK 混合检索阶段召回数量
     * @param rerankTopK rerank之后最终保留数量
     * @return 重排之后的子切片Document列表
     */
    public Mono<List<Document>> retrieve(String query,
                                         CurrentUserContext currentUserContext,
                                         SearchScope searchScope,
                                         int hybridTopK,
                                         int rerankTopK) {
        return retrieveInternal(query, currentUserContext, searchScope, hybridTopK, rerankTopK, true, true, Map.of());
    }

    /**
     * 对外API：带额外trace标签的检索
     * @param extraTags 自定义链路追踪标签
     */
    public Mono<List<Document>> retrieve(String query,
                                         CurrentUserContext currentUserContext,
                                         SearchScope searchScope,
                                         int hybridTopK,
                                         int rerankTopK,
                                         Map<String, Object> extraTags) {
        return retrieveInternal(query, currentUserContext, searchScope, hybridTopK, rerankTopK, true, true, extraTags);
    }

    /**
     * 对外API：检索并且扩展父块上下文（父子块检索模式）
     * 返回RetrievalResult：包含命中子块 + 对应的完整父块大段上下文，用于喂给LLM
     * @param query 用户提问
     * @param currentUserContext 用户上下文
     * @param searchScope 检索范围
     * @param hybridTopK 混合检索召回数
     * @param rerankTopK rerank截断数
     * @param extraTags trace额外标签
     * @return RetrievalResult 子候选集合 + 父上下文集合
     */
    public Mono<RetrievalResult> retrieveWithParentContexts(String query,
                                                            CurrentUserContext currentUserContext,
                                                            SearchScope searchScope,
                                                            int hybridTopK,
                                                            int rerankTopK,
                                                            Map<String, Object> extraTags) {
        return retrieveInternal(query, currentUserContext, searchScope, hybridTopK, rerankTopK, true, true, extraTags)
                // 拿到检索出来子chunk，回查MySQL父块
                .flatMap(childCandidates -> expandParentContexts(childCandidates)
                        .map(parentContexts -> new RetrievalResult(childCandidates, parentContexts)));
    }

    /**
     * 对外API：无用户权限版本，可开关稀疏检索、开关rerank，用于调试
     */
    public Mono<List<Document>> retrieve(String query,
                                         int hybridTopK,
                                         int rerankTopK,
                                         boolean useSparseSearch,
                                         boolean useRerank) {
        return retrieveInternal(query, null, SearchScope.empty(), hybridTopK, rerankTopK, useSparseSearch, useRerank, Map.of());
    }

    /**
     * 检索内部核心实现
     * 步骤：1.混合向量检索  2.可选Rerank重排，失败自动降级不重排
     * @param query 用户问题
     * @param currentUserContext 用户上下文，null代表关闭权限过滤
     * @param searchScope 检索空间、标签范围
     * @param hybridTopK 混合检索召回topK
     * @param rerankTopK rerank输出上限；关闭rerank时也用作截断数量
     * @param useSparseSearch 是否开启sparse稀疏向量检索
     * @param useRerank 是否开启重排序
     * @param extraTags 链路追踪附加标签
     * @return 响应式Mono，返回处理完成的子Document切片列表
     */
    private Mono<List<Document>> retrieveInternal(String query,
                                                  CurrentUserContext currentUserContext,
                                                  SearchScope searchScope,
                                                  int hybridTopK,
                                                  int rerankTopK,
                                                  boolean useSparseSearch,
                                                  boolean useRerank,
                                                  Map<String, Object> extraTags) {
        long searchStart = System.currentTimeMillis();
        // 组装链路追踪标签
        Map<String, Object> traceTags = new java.util.HashMap<>(Map.of(
                "rag.hybrid_topk", hybridTopK,
                "rag.rerank_topk", rerankTopK,
                "rag.filter_tags", searchScope == null ? "" : String.join(",", searchScope.requestedTags()),
                "rag.requested_spaces", searchScope == null ? "" : String.join(",", searchScope.requestedSpaceCodes())));
        traceTags.putAll(extraTags);

        // 执行混合检索，埋点追踪rag.hybrid_search
        return tracingSupport.traceMono("rag.hybrid_search", traceTags,
                        hybridSearchService.hybridSearch(query, currentUserContext, searchScope, hybridTopK, useSparseSearch))
                // 打印检索耗时debug日志
                .doOnNext(candidates -> log.debug("Hybrid search returned {} candidates in {}ms",
                        candidates.size(), System.currentTimeMillis() - searchStart))
                // 检索阶段异常日志
                .doOnError(error -> log.error("Retrieval stage failed for query='{}', searchScope={}",
                        query, searchScope, error))
                .flatMap(candidates -> {
                    // 召回结果为空，直接返回空集合
                    if (candidates == null || candidates.isEmpty()) {
                        return Mono.just(Collections.emptyList());
                    }
                    // 不开启Rerank：直接对原始候选做截断
                    if (!useRerank) {
                        // rerankTopK兼任截断上限：跳过重排序时直接按此数量截断候选集
                        return Mono.just(candidates.subList(0, Math.min(candidates.size(), rerankTopK)));
                    }
                    // 开启Rerank：调用重排序模型
                    long rerankStart = System.currentTimeMillis();
                    return tracingSupport.traceMono("rag.rerank",
                                    Map.of(
                                            "rag.candidate_count", candidates.size(),
                                            "rag.rerank_topk", rerankTopK),
                                    rerankService.rerank(query, candidates, rerankTopK))
                            .doOnNext(docs -> log.debug("Rerank returned {} docs in {}ms",
                                    docs.size(), System.currentTimeMillis() - rerankStart))
                            // rerank服务异常降级：reranker挂了，直接返回原始截断候选，系统不崩溃
                            .onErrorResume(error -> {
                                log.warn("Rerank failed, fallback to raw candidates: {}", error.getMessage());
                                return Mono.just(candidates.subList(0, Math.min(candidates.size(), rerankTopK)));
                            });
                });
    }

    /**
     * 子块回查父块：父子块增强检索核心逻辑
     * 将检索命中的子块按 parent_block_id 去重聚合，批量查询 MySQL 获取完整父块上下文
     * @param childCandidates 向量检索命中的子切片chunk列表
     * @return 父上下文块集合Mono
     */
    private Mono<List<ParentContextBlock>> expandParentContexts(List<Document> childCandidates) {
        if (childCandidates == null || childCandidates.isEmpty()) {
            return Mono.just(List.of());
        }

        // 按 parent_block_id 去重聚合，同时收集每个父块下所有命中子块的 evidence_id
        Map<String, ParentAccumulator> byParentId = new LinkedHashMap<>();
        int rank = 0;
        for (Document child : childCandidates) {
            rank++;
            Map<String, Object> metadata = child.getMetadata();
            String parentBlockId = stringValue(metadata.get("parent_block_id"));
            String evidenceId = evidenceId(child);
            // 元数据关键字缺失 → 索引损坏，抛出业务异常，提示重建文档索引
            if (!StringUtils.hasText(parentBlockId) || !StringUtils.hasText(evidenceId)) {
                return Mono.error(new ParentContextMissingException("知识库索引数据不一致，请重建该文档索引后重试。"));
            }
            int currentRank = rank;
            // 首次遇到该 parent_block_id → 创建累加器，记录最佳排名
            // 重复遇到 → 仅追加 evidence_id（同一父块下的不同子块均被命中）
            ParentAccumulator accumulator = byParentId.computeIfAbsent(parentBlockId,
                    ignored -> new ParentAccumulator(parentBlockId, stringValue(metadata.get("doc_uuid")), currentRank));
            accumulator.evidenceIds().add(evidenceId);
        }

        // 批量查询 MySQL，一次取出所有去重后的父块，减少IO次数
        List<String> parentBlockIds = new ArrayList<>(byParentId.keySet());
        return parentBlockService.findByParentBlockIds(parentBlockIds)
                .map(parentBlocks -> toParentContextBlocks(byParentId, parentBlocks));
    }

    /**
     * 将MySQL查询结果与内存累加器合并，校验schema版本和docUuid一致性
     * 做数据一致性校验：防止Milvus向量与MySQL父块数据不同步
     * @param accumulators 内存中父块id对应的命中子块信息累加器
     * @param parentBlocks MySQL批量查询返回父块map key:parentBlockId
     * @return 组装好对外输出ParentContextBlock
     */
    private List<ParentContextBlock> toParentContextBlocks(Map<String, ParentAccumulator> accumulators,
                                                           Map<String, KnowledgeParentBlock> parentBlocks) {
        List<ParentContextBlock> contexts = new ArrayList<>();
        for (ParentAccumulator accumulator : accumulators.values()) {
            KnowledgeParentBlock parentBlock = parentBlocks.get(accumulator.parentBlockId());
            // 校验：父块必须存在、schema版本匹配、docUuid一致（防止跨文档误关联）
            if (parentBlock == null
                    || parentBlock.getChunkSchemaVersion() == null
                    || parentBlock.getChunkSchemaVersion() != KnowledgeParentBlockService.CHUNK_SCHEMA_VERSION
                    || !Objects.equals(parentBlock.getDocUuid(), accumulator.docUuid())) {
                throw new ParentContextMissingException("知识库索引数据不一致，请重建该文档索引后重试。");
            }
            contexts.add(new ParentContextBlock(
                    parentBlock.getParentBlockId(),
                    parentBlock.getDocUuid(),
                    parentBlock.getFileName(),
                    parentBlock.getContent(),          // 1200 字完整段落，发给 LLM 推理
                    parentBlock.getParentIndex(),
                    parentBlock.getPageStart(),
                    parentBlock.getPageEnd(),
                    List.copyOf(accumulator.evidenceIds()),  // 该父块下所有被命中的子块 evidence_id，供 LLM 引用
                    accumulator.bestRank()));                  // 该父块下最佳排名的子块排名，用于最终排序
        }
        return contexts;
    }

    /**
     * 获取证据ID：优先取metadata中的evidence_id，没有就用document id兜底
     * @param document spring ai Document
     * @return evidenceId字符串
     */
    private String evidenceId(Document document) {
        if (document == null) {
            return null;
        }
        Object metadataEvidenceId = document.getMetadata().get("evidence_id");
        if (metadataEvidenceId != null && StringUtils.hasText(metadataEvidenceId.toString())) {
            return metadataEvidenceId.toString().trim();
        }
        return document.getId();
    }

    /**
     * 对象安全转为字符串，null返回null
     */
    private String stringValue(Object value) {
        return value == null ? null : value.toString();
    }

    /**
     * 内存record记录：父块累加器
     * 同一个parentBlockId，会命中多个子chunk；记录父块ID、文档uuid、最优排名、全部命中子块证据id
     */
    private record ParentAccumulator(
            String parentBlockId,
            String docUuid,
            int bestRank,              // 该父块下最早被命中的子块排名（数字越小越靠前）
            List<String> evidenceIds   // 该父块下所有被命中子块的 evidence_id 集合
    ) {
        // 构造器：初始化时实例化空ArrayList存放evidenceIds
        private ParentAccumulator(String parentBlockId, String docUuid, int bestRank) {
            this(parentBlockId, docUuid, bestRank, new ArrayList<>());
        }
    }
}
