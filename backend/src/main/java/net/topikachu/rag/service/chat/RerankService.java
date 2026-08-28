package net.topikachu.rag.service.chat;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.document.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Rerank service using local BGE Reranker (BAAI/bge-reranker-base) via TEI.
 * Includes circuit breaker for resilience.
 * 基于TEI部署的BGE重排模型服务
 * 具备熔断降级能力，用于RAG流程中对向量检索出来的文档做二次精排
 */
@Service
@Slf4j
public class RerankService {

    /**
     * TEI rerank接口地址，默认本地8099端口
     * 配置项：rag.rerank.url
     */
    @Value("${rag.rerank.url:http://localhost:8099/rerank}")
    private String rerankUrl;

    /**
     * 重排接口超时时间，单位毫秒，默认1500ms
     * 配置项：rag.rerank.timeout-ms
     */
    @Value("${rag.rerank.timeout-ms:1500}")
    private int timeoutMs;

    private final WebClient webClient;

    /**
     * 构造注入WebClient.Builder，构建web客户端用于http调用rerank接口
     * @param webClientBuilder web客户端构建器
     */
    public RerankService(WebClient.Builder webClientBuilder) {
        this.webClient = webClientBuilder.build();
    }

    /**
     * 响应式重排接口，设置硬超时，避免阻塞请求线程
     * Resilience4j：熔断器 + 信号量舱壁，失败自动走fallback降级逻辑
     * @param query 用户查询问题
     * @param docs 向量检索返回的原始文档列表
     * @param topN 重排之后保留返回的文档数量
     * @return Mono包装重排完成的文档列表，文档metadata会写入rerank_score重排分数
     */
    @io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker(name = "rerankService", fallbackMethod = "rerankFallback")
    @io.github.resilience4j.bulkhead.annotation.Bulkhead(name = "rerankService", type = io.github.resilience4j.bulkhead.annotation.Bulkhead.Type.SEMAPHORE, fallbackMethod = "rerankFallback")
    public Mono<List<Document>> rerank(String query, List<Document> docs, int topN) {
        // 文档为空直接返回空集合，不调用rerank服务
        if (docs == null || docs.isEmpty()) {
            return Mono.just(new ArrayList<>());
        }

        // 记录方法开始时间，用于统计耗时
        long startTime = System.currentTimeMillis();

        // 提取所有文档的文本内容，传给TEI重排接口
        List<String> texts = docs.stream()
                .map(Document::getText)
                .collect(Collectors.toList());

        // 组装TEI rerank请求体
        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("query", query);
        requestBody.put("texts", texts); // TEI接口参数名是texts，不是documents
        // truncate=false：长文本不截断，保证Cross‑Encoder读取完整上下文；截断会丢失关键信息影响重排效果
        requestBody.put("truncate", false);

        return webClient.post()
                .uri(rerankUrl)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(requestBody) // 请求体写入
                .retrieve()
                .bodyToMono(JsonNode.class) // 接收json响应
                .timeout(Duration.ofMillis(timeoutMs)) // 设置响应超时，超时抛出TimeoutException
                .map(results -> {
                    List<Document> rerankedDocs = new ArrayList<>();
                    // 判断返回结果不为空并且是数组格式
                    if (results != null && results.isArray()) {
                        // 存储【原始文档下标，重排得分】
                        List<Map.Entry<Integer, Double>> scored = new ArrayList<>();
                        for (JsonNode result : results) {
                            int index = result.get("index").asInt(); // 对应传入texts数组的下标
                            double score = result.get("score").asDouble(); // rerank相似度得分，分数越高越相关
                            scored.add(Map.entry(index, score));
                        }
                        // 按score降序排序，分数高的排在前面
                        scored.sort((a, b) -> Double.compare(b.getValue(), a.getValue()));

                        // 取topN个文档，组装返回结果
                        for (int i = 0; i < Math.min(scored.size(), topN); i++) {
                            int index = scored.get(i).getKey();
                            // 下标越界防护，跳过非法下标
                            if (index < 0 || index >= docs.size()) {
                                continue;
                            }
                            double score = scored.get(i).getValue();
                            Document originalDoc = docs.get(index);
                            // 将rerank分数写入文档元数据，方便后续链路查看分数
                            originalDoc.getMetadata().put("rerank_score", score);
                            rerankedDocs.add(originalDoc);
                        }
                    }
                    return rerankedDocs;
                })
                .doOnNext(rerankedDocs -> {
                    // 成功时打印重排耗时以及返回文档数量
                    long elapsed = System.currentTimeMillis() - startTime;
                    log.info("Rerank completed in {}ms, returned {} docs", elapsed, rerankedDocs.size());
                })
                .doOnError(e -> {
                    // 异常分支日志打印，区分超时异常与其他异常
                    long elapsed = System.currentTimeMillis() - startTime;
                    if (e instanceof TimeoutException) {
                        log.warn("Rerank timeout after {}ms (limit={}ms), triggering circuit breaker", elapsed,
                                timeoutMs);
                    } else {
                        log.error("Rerank failed after {}ms, triggering circuit breaker: {}", elapsed, e.getMessage(),
                                e);
                    }
                });
    }

    /**
     * Rerank服务降级fallback方法
     * 当rerank调用超时、报错、熔断器打开时进入该逻辑
     * 策略：放弃重排，直接返回向量检索原始结果的前topN条
     * @param query 用户查询
     * @param docs 原始文档列表
     * @param topN 需要返回文档数
     * @param t 捕获到的异常
     * @return Mono包装原始截取后的文档列表
     */
    public Mono<List<Document>> rerankFallback(String query, List<Document> docs, int topN, Throwable t) {
        log.warn("▇▇ Rerank 降级触发 ▇▇ 原因: {} - 仅返回 Top{} 原始检索结果", t.getMessage(), topN);

        // 空文档防护
        if (docs == null || docs.isEmpty()) {
            return Mono.just(Collections.emptyList());
        }
        // 截取原始检索前topN文档直接返回，跳过重排
        return Mono.just(docs.subList(0, Math.min(docs.size(), topN)));
    }
}
