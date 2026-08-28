package net.topikachu.rag.service.chat;

import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.auth.CurrentUserContext;
import net.topikachu.rag.auth.SearchScope;
import net.topikachu.rag.evaluation.ContextNode;
import net.topikachu.rag.evaluation.EvaluationConfig;
import net.topikachu.rag.evaluation.EvaluationResultItem;
import net.topikachu.rag.observability.TracingSupport;
import net.topikachu.rag.service.chat.strategy.ChatModelStrategyFactory;
import org.springframework.ai.document.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Chat service with hybrid search (dense + sparse) and reranking.
 * RAG门面服务：封装线上对话 + 离线评估两大能力；响应式WebFlux
 * 1. streamWithSources：线上RAG问答，父子块检索 + 带来源引用的答案生成
 * 2. evaluateQuery：离线消融实验/评估脚本使用，支持重试、模型降级兜底
 */
@Service
@Slf4j
public class ChatService {

    // 检索流水线：混合检索(稠密向量+稀疏BM25)、重排序、父子块组装逻辑
    private final RetrievalPipeline retrievalPipeline;
    // 模型策略工厂：适配多种大模型，获取对应ChatClient
    private final ChatModelStrategyFactory strategyFactory;
    // 响应式大模型调用网关：评估模式下调用LLM
    private final ReactiveChatGateway reactiveChatGateway;
    // 链路追踪工具：生成traceId，全链路日志串联排查问题
    private final TracingSupport tracingSupport;
    // 带事实来源的回答生成模块：组装prompt、调用LLM、解析输出answer+usedSources
    private final GroundedTurnModule groundedTurnModule;

    // 混合检索先召回候选数量：稠密+稀疏检索，先取出20条文档
    @Value("${rag.retrieval.hybrid-topk:20}")
    private int hybridTopK;

    // rerank重排序之后，最终送入LLM的文档数量
    @Value("${rag.retrieval.rerank-topk:10}")
    private int rerankTopK;

    // 送入大模型的上下文总字符上限，防止prompt过长；TODO 当前按字符，非真实token计数
    @Value("${rag.retrieval.max-context-chars:40000}")
    private int maxContextChars;

    // 构造函数注入全部依赖
    public ChatService(RetrievalPipeline retrievalPipeline,
                       ChatModelStrategyFactory strategyFactory,
                       ReactiveChatGateway reactiveChatGateway,
                       TracingSupport tracingSupport,
                       GroundedTurnModule groundedTurnModule) {
        this.retrievalPipeline = retrievalPipeline;
        this.strategyFactory = strategyFactory;
        this.reactiveChatGateway = reactiveChatGateway;
        this.tracingSupport = tracingSupport;
        this.groundedTurnModule = groundedTurnModule;
    }

    /**
     * 返回给上层Controller的响应体记录
     * @param flux 字符串流；RAG模式下Flux.just(完整答案)，不是逐token流式；Agent才是真实token流
     * @param usedSources 当前回答引用到的文档来源列表，前端展示参考文献
     */
    public record ChatStreamResponse(Flux<String> flux, List<UsedSource> usedSources) {
    }

    /**
     * 评估模块专用检索方法
     * @param query 用户问题
     * @param useSparseSearch 是否开启稀疏BM25检索
     * @param useRerank 是否开启重排序
     * @param topK 最终保留多少条文档
     * @return Mono包装的文档列表
     */
    public Mono<List<Document>> retrieveForEvaluation(String query, boolean useSparseSearch, boolean useRerank, int topK) {
        // 如果开启rerank，需要多召回一部分文档，重排后再截断
        int fetchK = useRerank ? hybridTopK : topK;
        return retrievalPipeline.retrieve(query, fetchK, topK, useSparseSearch, useRerank);
    }

    // TODO:: 精确计算token消耗量，当前仅做字符截断，字符不能等价token
    /**
     * 线上RAG问答主入口；问答生成链 ——答案出去
     * @param userInput 用户提问文本
     * @param conversationId 会话ID，标识同一个聊天窗口
     * @param currentUserContext 当前登录用户上下文：userId、username
     * @param searchScope 检索权限：允许访问的知识库空间、标签
     * @param modelId 指定使用哪个大模型
     * @param msgId 当前这条消息唯一ID
     * @return Mono<ChatStreamResponse> 异步返回应答对象；flux只发送一次完整答案
     */
    public Mono<ChatStreamResponse> streamWithSources(String userInput, String conversationId,
                                                      CurrentUserContext currentUserContext, SearchScope searchScope,
                                                      String modelId, String msgId) {
        // 打印入参日志，线上排查问题使用；空判断防止searchScope为null空指针
        log.info("Processing query: '{}', conversationId: {}, spaces: {}, tags: {}, modelId: {}, user={}",
                userInput, conversationId,
                searchScope == null ? List.of() : searchScope.requestedSpaceCodes(),
                searchScope == null ? List.of() : searchScope.requestedTags(),
                modelId,
                currentUserContext == null ? null : currentUserContext.username());

        // 获取全链路traceId，透传给下游所有组件，日志可以串联整条请求
        String traceId = tracingSupport.getCurrentTraceId();

        // 1.父子块检索：向量库检索子块，再拿到对应父级完整上下文；附加埋点元数据
        return retrievalPipeline.retrieveWithParentContexts(userInput, currentUserContext, searchScope, hybridTopK, rerankTopK,
                        Map.of(
                                "chat.mode", "rag",
                                "chat.model_id", modelId == null ? "" : modelId,
                                "chat.conversation_id", conversationId == null ? "" : conversationId))
                // flatMap：上一步检索完成，拿到retrievalResult，再执行下一个异步IO（调用LLM生成答案）
                .flatMap(retrievalResult -> groundedTurnModule.execute(new GroundedTurnModule.Command(
                        userInput,
                        conversationId,
                        currentUserContext.userId(),
                        modelId,
                        "rag", //标记运行模式为RAG
                        msgId,
                        traceId,
                        retrievalResult.childCandidates(),  //向量匹配命中的小子块
                        retrievalResult.parentContexts()))) //小子块对应的父完整上下文
                // map：同步转换，不发起IO；封装输出对象
                .map(result -> new ChatStreamResponse(
                        Flux.just(result.answer()), // ⚠️RAG模式：完整答案一次性包装Flux，非token流式
                        result.usedSources()));     //本次回答引用的文档来源
    }

    /**
     * Non‑streaming, strict evaluation method used by AblationStudyRunner.
     * Extracts ContextNodes with metadata and applies dynamic evaluation
     * configurations.
     * 评估方法重载1：对外简化入口，使用默认rerankTopK、允许模型降级
     */
    public Mono<EvaluationResultItem> evaluateQuery(
            String question,
            String groundTruth,
            EvaluationConfig config,
            Resource baselinePromptResource,
            Resource optimizedPromptResource,
            String modelId) {
        return evaluateQuery(question, groundTruth, config, baselinePromptResource, optimizedPromptResource, rerankTopK,
                modelId, true);
    }

    /**
     * 评估方法重载2：自定义topK，允许模型降级
     */
    public Mono<EvaluationResultItem> evaluateQuery(
            String question,
            String groundTruth,
            EvaluationConfig config,
            Resource baselinePromptResource,
            Resource optimizedPromptResource,
            int topK,
            String modelId) {
        return evaluateQuery(question, groundTruth, config, baselinePromptResource, optimizedPromptResource, topK,
                modelId, true);
    }

    /**
     * 评估核心实现方法；离线消融实验脚本调用；没有父子块逻辑；支持重试+ollama降级兜底
     * @param question 测试问题
     * @param groundTruth 标准答案
     * @param config 评估配置：开关稀疏检索、开关rerank、切换prompt版本
     * @param baselinePromptResource 基线版prompt模板文件资源
     * @param optimizedPromptResource 优化版prompt模板文件资源
     * @param topK 检索返回文档数量
     * @param modelId 指定模型
     * @param allowModelFallback 是否允许失败后降级到ollama本地模型
     * @return EvaluationResultItem：问题、标准答案、模型输出、检索上下文节点集合
     */
    public Mono<EvaluationResultItem> evaluateQuery(
            String question,
            String groundTruth,
            EvaluationConfig config,
            Resource baselinePromptResource,
            Resource optimizedPromptResource,
            int topK,
            String modelId,
            boolean allowModelFallback) {
        // 记录开始时间，统计单次评估耗时
        long startTime = System.currentTimeMillis();

        // 第一步执行检索获取文档列表
        return retrieveForEvaluation(question, config.useSparseSearch(), config.useRerank(), topK)
                // 检索完成拿到docs，Mono.defer：延迟创建内部Mono，保证每次订阅重新执行逻辑
                .flatMap(docs -> Mono.defer(() -> {
                    // ContextNode：评估对象，保存文档文本、文件名、检索得分，后续用来计算评估指标
                    List<ContextNode> contextNodes = new ArrayList<>();
                    // 拼接送入LLM的全部上下文字符串
                    StringBuilder contextTextBuilder = new StringBuilder();

                    // 遍历检索得到的文档，拼接上下文
                    for (int i = 0; i < docs.size(); i++) {
                        Document doc = docs.get(i);
                        // 从文档元数据取出文件名，没有就默认Unknown File
                        String fileName = (String) doc.getMetadata().getOrDefault("file_name", "Unknown File");
                        // 获取检索打分score，做类型安全判断
                        Object scoreObj = doc.getMetadata().get("score");
                        Double score = (scoreObj instanceof Number) ? ((Number) scoreObj).doubleValue() : 0.0;

                        // 存入评估节点
                        contextNodes.add(new ContextNode(doc.getText(), fileName, score));

                        // 格式化文档片段，带上来源标记
                        String structuredEntry = String.format("【文档来源: %s】\n内容: %s\n------------------------\n",
                                fileName, doc.getText());

                        // 超过最大字符限制直接停止拼接，防止prompt超长
                        if (contextTextBuilder.length() + structuredEntry.length() > maxContextChars) {
                            break;
                        }
                        contextTextBuilder.append(structuredEntry);
                    }

                    // 拼接完成的上下文字符串
                    String contextText = contextTextBuilder.toString();
                    // 根据配置读取基线或者优化版prompt模板
                    String systemPromptText = loadPromptText(config, baselinePromptResource, optimizedPromptResource);

                    // 调用大模型，传入systemPrompt，占位符map填充context和question
                    return reactiveChatGateway.call(
                                    strategyFactory.getStrategy(modelId).getChatClient(),
                                    systemPromptText,
                                    Map.of("context", contextText, "question", question),
                                    question)
                            // 拿到模型生成回答，封装评估返回对象
                            .map(generatedAnswer -> {
                                log.info("Evaluation query completed in {}ms. Length of answer: {}",
                                        System.currentTimeMillis() - startTime, generatedAnswer.length());
                                return new EvaluationResultItem(
                                        question,
                                        groundTruth,
                                        generatedAnswer,
                                        contextNodes);
                            });
                }))
                // 异常重试配置：最多重试3次，每次间隔2秒退避；只对RuntimeException重试
                .retryWhen(reactor.util.retry.Retry.backoff(3, java.time.Duration.ofSeconds(2))
                        .filter(throwable -> {
                            // Can add specific sub‑exceptions if needed, but for now retry on most runtime
                            // exceptions from APIs
                            return throwable instanceof RuntimeException;
                        })
                        // 重试前打印警告日志，记录第几次重试
                        .doBeforeRetry(retrySignal -> log.warn("[{}] API error '{}', retrying... ({}/{})",
                                modelId, retrySignal.failure().getMessage(), retrySignal.totalRetriesInARow() + 1, 3)))
                // 全部重试失败进入错误兜底分支
                .onErrorResume(e -> {
                    log.error("[{}] API definitively failed after retries: {}", modelId, e.getMessage());
                    // 如果允许降级，并且当前不是ollama，则递归调用自己，强制切换modelId=ollama本地模型
                    if (allowModelFallback && !"ollama".equals(modelId)) {
                        log.warn("=== 触发降级机制 === 切换至本地大模型 (ollama) 作兜底回复");
                        // Recursively call evaluateQuery but force the modelId to be 'ollama'
                        return evaluateQuery(question, groundTruth, config, baselinePromptResource,
                                optimizedPromptResource, topK, "ollama", true);
                    }
                    // 不允许降级，直接把异常向外抛出
                    if (!allowModelFallback) {
                        return Mono.error(e);
                    }
                    // 终极兜底：ollama也失败，返回一段错误文本作为answer，评估脚本可以继续运行不会中断
                    return Mono.just(new EvaluationResultItem(
                            question,
                            groundTruth,
                            "【系统提示】所有模型调用均失败，请稍后再试或联系管理员。错误详情: " + e.getMessage(),
                            new ArrayList<>()));
                });
    }

    /**
     * 读取prompt模板文件
     * @param config 评估配置，决定取基线还是优化prompt
     * @param baselinePromptResource 基线prompt资源
     * @param optimizedPromptResource 优化prompt资源
     * @return 读取出来的prompt文本；IO异常返回兜底默认prompt
     */
    private String loadPromptText(EvaluationConfig config, Resource baselinePromptResource,
                                  Resource optimizedPromptResource) {
        try {
            // 根据配置选择使用哪一套prompt模板
            Resource promptResource = config.useOptimizedPrompt()
                    ? optimizedPromptResource
                    : baselinePromptResource;
            // 读取资源文件，UTF‑8编码
            return promptResource.getContentAsString(StandardCharsets.UTF_8);
        } catch (java.io.IOException e) {
            log.error("Failed to load prompt template, falling back to default.", e);
            // 文件读取异常，返回兜底简单prompt字符串
            return "Context:\n{context}";
        }
    }

}
