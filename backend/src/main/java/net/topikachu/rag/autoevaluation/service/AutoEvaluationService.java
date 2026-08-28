package net.topikachu.rag.autoevaluation.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.autoevaluation.dto.*;
import net.topikachu.rag.autoevaluation.dto.RagasEvaluationRequest.RagasEvaluationItem;
import net.topikachu.rag.autoevaluation.dto.RagasEvaluationResponse.RagasEvaluationResult;
import net.topikachu.rag.autoevaluation.entity.ChatEvaluationAutoEntity;
import net.topikachu.rag.autoevaluation.entity.ChatEvaluationAutoRunEntity;
import net.topikachu.rag.autoevaluation.mapper.ChatEvaluationAutoLockMapper;
import net.topikachu.rag.autoevaluation.mapper.ChatEvaluationAutoMapper;
import net.topikachu.rag.autoevaluation.mapper.ChatEvaluationAutoRunMapper;
import net.topikachu.rag.evaluation.entity.ChatEvaluationEntity;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * RAGAS 自动评估服务
 *
 * 核心功能：
 * 1. 定时从数据库中抽样对话记录
 * 2. 调用 RAGAS 服务进行质量评估（6项指标：忠实度、相关性、精确度等）
 * 3. 保存评估结果并计算平均分
 * 4. 提供统计查询接口
 *
 * 设计特点：
 * - 分布式锁：防止多实例同时执行评估任务
 * - 分批处理：避免单次请求数据量过大
 * - 串行执行：保护 RAGAS 服务不被压垮
 * - 异常兜底：单条失败不影响整体流程
 * - 超时保护：自动标记卡住的任务为失败
 */
@Service
@Slf4j
public class AutoEvaluationService {

    // ==================== 常量定义 ====================

    /** 分布式锁名称 */
    private static final String LOCK_NAME = "auto_evaluation";

    /** 无参考答案时的占位符（用于去重） */
    private static final String NO_REFERENCE = "NO_REFERENCE";

    /** 任务状态：运行中 */
    private static final String RUNNING = "RUNNING";

    /** 任务状态：已完成 */
    private static final String COMPLETED = "COMPLETED";

    /** 任务状态：失败 */
    private static final String FAILED = "FAILED";

    // ==================== 依赖注入 ====================

    /** 自动评估 Mapper：操作评估明细表 */
    private final ChatEvaluationAutoMapper autoMapper;

    /** 自动评估运行记录 Mapper：操作任务运行表 */
    private final ChatEvaluationAutoRunMapper runMapper;

    /** 分布式锁 Mapper：操作锁表 */
    private final ChatEvaluationAutoLockMapper lockMapper;

    /** RAGAS HTTP 客户端：调用 RAGAS 打分服务 */
    private final RagasEvaluationClient ragasClient;

    /** JSON 解析器：解析数据库中的 JSON 字段 */
    private final ObjectMapper objectMapper;

    // ==================== 配置参数 ====================

    /** 抽样数量：每次任务最多抽取多少条样本 */
    private final int sampleSize;

    /** 批次大小：每批发送多少条给 RAGAS */
    private final int batchSize;

    /** 样本时间窗口：只取最近 N 小时的对话 */
    private final int sampleWindowHours;

    /** 任务超时时间（分钟）：超过此时间认为任务卡死 */
    private final int runTimeoutMinutes;

    /** 当前实例 ID：用于分布式锁标识 */
    private final String instanceId;

    // ==================== 构造函数 ====================

    public AutoEvaluationService(
            ChatEvaluationAutoMapper autoMapper,
            ChatEvaluationAutoRunMapper runMapper,
            ChatEvaluationAutoLockMapper lockMapper,
            RagasEvaluationClient ragasClient,
            ObjectMapper objectMapper,
            @Value("${rag.evaluation.auto.sample-size:20}") int sampleSize,
            @Value("${rag.evaluation.auto.batch-size:5}") int batchSize,
            @Value("${rag.evaluation.auto.sample-window-hours:24}") int sampleWindowHours,
            @Value("${rag.evaluation.auto.run-timeout-minutes:120}") int runTimeoutMinutes) {
        this.autoMapper = autoMapper;
        this.runMapper = runMapper;
        this.lockMapper = lockMapper;
        this.ragasClient = ragasClient;
        this.objectMapper = objectMapper;
        this.sampleSize = sampleSize;
        this.batchSize = Math.max(1, batchSize);  // 至少为1
        this.sampleWindowHours = sampleWindowHours;
        this.runTimeoutMinutes = runTimeoutMinutes;
        this.instanceId = resolveInstanceId();  // 解析主机名作为实例ID
    }

    // ==================== 核心方法：手动触发评估 ====================

    /**
     * 手动触发 RAGAS 自动评估
     *
     * 流程：
     * 1. 获取分布式锁（防止多实例同时执行）
     * 2. 插入任务记录（状态：RUNNING）
     * 3. 异步执行 doRun（不阻塞当前线程）
     * 4. 立即返回 runId 给调用方
     *
     * @return Mono<String> 本轮任务ID
     */
    public Mono<String> runAutoEvaluation() {
        return Mono.fromCallable(() -> {
                    // ----- 1. 获取分布式锁 -----
                    String ownerId = instanceId + "-" + newRunId();
                    if (!acquireLock(ownerId)) {
                        throw new IllegalStateException("Auto evaluation is already running.");
                    }

                    // ----- 2. 插入任务记录 -----
                    String runId = newRunId();
                    ChatEvaluationAutoRunEntity run = new ChatEvaluationAutoRunEntity();
                    run.setRunId(runId);
                    run.setStatus(RUNNING);
                    run.setStartedAt(LocalDateTime.now());
                    runMapper.insert(run);

                    // ----- 3. 异步执行评估任务 -----
                    // 关键：.subscribe() 触发异步执行，当前线程立即返回
                    // doRun 返回 Mono<Void>，在后台线程池执行
                    doRun(runId)
                            .doOnError(e -> log.error("RAGAS auto evaluation run failed runId={}", runId, e))
                            .onErrorResume(e -> markRunFailed(runId, e.getMessage()).then())  // 异常时标记失败
                            .doFinally(signal -> releaseLock(ownerId))  // 无论成功/失败/取消，都释放锁
                            .subscribe();  // 🔥 触发异步执行，不阻塞！

                    // ----- 4. 立即返回 runId -----
                    return runId;
                })
                .subscribeOn(Schedulers.boundedElastic());  // 数据库操作在弹性线程池执行
    }

    // ==================== 查询接口 ====================

    /**
     * 查询自动评估统计数据
     *
     * @return Mono<AutoStatsResult> 包含：最近一次运行记录、全局平均分、趋势数据
     */
    public Mono<AutoStatsResult> queryAutoStats() {
        return Mono.fromCallable(() -> {
                    // 1. 查询最近一次运行记录
                    ChatEvaluationAutoRunEntity lastRun = runMapper.selectLastRun();

                    // 2. 查询全局汇总数据（所有历史评估的平均分）
                    Map<String, Object> summary = autoMapper.selectAutoSummary();

                    // 3. 查询趋势数据（按天的平均分变化）
                    List<RagasTrendPoint> trend = autoMapper.selectAutoTrend().stream()
                            .map(row -> new RagasTrendPoint(
                                    String.valueOf(row.get("day")),
                                    toNullableDouble(row.get("faithfulness")),
                                    toNullableDouble(row.get("answer_relevancy")),
                                    toNullableDouble(row.get("context_precision")),
                                    toNullableDouble(row.get("context_recall")),
                                    toNullableDouble(row.get("answer_correctness")),
                                    toNullableDouble(row.get("answer_similarity"))))
                            .toList();

                    // 4. 组装返回结果
                    return new AutoStatsResult(
                            toRunItem(lastRun),
                            toLong(summary.get("total_evaluated")),
                            toNullableDouble(summary.get("avg_faithfulness")),
                            toNullableDouble(summary.get("avg_answer_relevancy")),
                            toNullableDouble(summary.get("avg_context_precision")),
                            toNullableDouble(summary.get("avg_context_recall")),
                            toNullableDouble(summary.get("avg_answer_correctness")),
                            toNullableDouble(summary.get("avg_answer_similarity")),
                            trend);
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 查询某个样本的所有历史评分记录
     *
     * @param evaluationId 样本ID
     * @return Mono<List<AutoScoreItem>> 该样本的所有评分记录
     */
    public Mono<List<AutoScoreItem>> queryScoresByEvaluationId(String evaluationId) {
        return Mono.fromCallable(() -> autoMapper.selectList(
                                com.baomidou.mybatisplus.core.toolkit.Wrappers
                                        .<ChatEvaluationAutoEntity>lambdaQuery()
                                        .eq(ChatEvaluationAutoEntity::getEvaluationId, evaluationId)
                                        .orderByDesc(ChatEvaluationAutoEntity::getCreateDate))
                        .stream()
                        .map(this::toScoreItem)
                        .toList())
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 查询所有运行记录的历史
     *
     * @return Mono<List<AutoRunItem>> 所有运行记录
     */
    public Mono<List<AutoRunItem>> queryRunHistory() {
        return Mono.fromCallable(() -> runMapper.selectRunHistory().stream()
                        .map(this::toRunItem)
                        .toList())
                .subscribeOn(Schedulers.boundedElastic());
    }

    // ==================== 定时任务 ====================

    /**
     * 定时执行自动评估
     *
     * 默认每天凌晨4点执行（可配置）
     * 如果上一个任务还在运行，会自动跳过（分布式锁保证）
     */
    @Scheduled(cron = "${rag.evaluation.auto.cron:0 0 4 * * ?}")
    public void scheduledRun() {
        runAutoEvaluation()
                .doOnSuccess(runId -> log.info("Scheduled RAGAS auto evaluation started runId={}", runId))
                .doOnError(e -> log.warn("Scheduled RAGAS auto evaluation skipped or failed to start: {}", e.getMessage()))
                .onErrorResume(e -> Mono.empty())
                .subscribe();
    }

    /**
     * 定时清理卡住的任务
     *
     * 每10分钟执行一次
     * 把 started_at 超过 runTimeoutMinutes 分钟且状态为 RUNNING 的任务标记为 FAILED
     */
    @Scheduled(cron = "0 0/10 * * * ?")
    public void markStaleRunsFailed() {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime threshold = now.minusMinutes(runTimeoutMinutes);
        int rows = runMapper.markStaleFailed(
                threshold,
                now,
                "Run timed out (" + runTimeoutMinutes + " minutes)");
        if (rows > 0) {
            log.warn("Marked {} stale RAGAS auto evaluation runs as FAILED", rows);
        }
    }

    // ==================== 核心评估逻辑 ====================

    /**
     * 执行评估任务
     *
     * 流程：
     * 1. 从数据库抽样（sampleSize 条，最近 sampleWindowHours 小时）
     * 2. 分批（每批 batchSize 条）
     * 3. 串行处理每个批次（concatMap）
     * 4. 全部完成后，计算平均分并更新任务状态
     *
     * @param runId 任务ID
     * @return Mono<Void> 完成信号
     */
    private Mono<Void> doRun(String runId) {
        // 线程安全计数器：统计成功/失败的样本数
        AtomicInteger successCount = new AtomicInteger();
        AtomicInteger failureCount = new AtomicInteger();

        return Mono.fromCallable(() -> {
                    // 1. 计算时间窗口起点：当前时间 - sampleWindowHours 小时
                    LocalDateTime since = LocalDateTime.now().minusHours(sampleWindowHours);

                    // 2. 从数据库抽样
                    List<ChatEvaluationEntity> samples = autoMapper.selectSamples(since, sampleSize);

                    // 3. 更新任务总样本数
                    runMapper.updateTotalSamples(runId, samples.size());

                    // 4. 分批
                    return partition(samples, batchSize);
                })
                .subscribeOn(Schedulers.boundedElastic())  // 数据库操作在弹性线程池
                .flatMapMany(Flux::fromIterable)           // List<List> → Flux<List>
                .concatMap(batch -> evaluateBatch(batch, runId, successCount, failureCount))  // 串行处理每个批次
                .then(Mono.fromRunnable(() -> completeRun(   // 所有批次完成后，更新任务状态
                                runId,
                                successCount.get(),
                                failureCount.get()))
                        .subscribeOn(Schedulers.boundedElastic()))
                .then();
    }

    /**
     * 处理单个批次
     *
     * @param batch 一批样本
     * @param runId 任务ID
     * @param successCount 成功计数器
     * @param failureCount 失败计数器
     * @return Mono<Void> 本批次完成信号
     */
    private Mono<Void> evaluateBatch(
            List<ChatEvaluationEntity> batch,
            String runId,
            AtomicInteger successCount,
            AtomicInteger failureCount) {
        // 1. 构建 ID → 实体 映射（用于 RAGAS 返回后找到原始实体）
        Map<String, ChatEvaluationEntity> byId = batch.stream()
                .collect(Collectors.toMap(ChatEvaluationEntity::getId, Function.identity()));

        // 2. 转成 RAGAS 请求格式
        RagasEvaluationRequest request = new RagasEvaluationRequest(batch.stream()
                .map(this::toRagasItem)
                .toList());

        // 3. 调用 RAGAS 服务
        return ragasClient.evaluate(request)
                // 4. 请求成功：保存结果
                .flatMap(response -> Mono.fromRunnable(() -> persistBatchResults(
                                response,
                                byId,
                                runId,
                                successCount,
                                failureCount))
                        .subscribeOn(Schedulers.boundedElastic())  // 数据库操作在弹性线程池
                        .then())
                // 5. 请求失败：整批标记为失败
                .onErrorResume(e -> {
                    failureCount.addAndGet(batch.size());
                    log.warn("RAGAS batch failed size={}: {}", batch.size(), e.getMessage());
                    return Mono.<Void>empty();  // 不中断整体流程
                });
    }

    // ==================== 结果处理 ====================

    /**
     * 持久化 RAGAS 返回的评分结果
     *
     * @param response RAGAS 响应
     * @param byId ID → 实体映射
     * @param runId 任务ID
     * @param successCount 成功计数器
     * @param failureCount 失败计数器
     */
    private void persistBatchResults(
            RagasEvaluationResponse response,
            Map<String, ChatEvaluationEntity> byId,
            String runId,
            AtomicInteger successCount,
            AtomicInteger failureCount) {
        Set<String> seen = new HashSet<>();  // 记录 RAGAS 返回了哪些 ID
        List<RagasEvaluationResult> results = response != null && response.items() != null
                ? response.items()
                : List.of();

        for (RagasEvaluationResult result : results) {
            seen.add(result.evaluationId());  // 记录返回的 ID
            ChatEvaluationEntity evaluation = byId.get(result.evaluationId());
            if (evaluation == null) {
                continue;  // 找不到对应实体，跳过
            }

            // 单条错误处理
            if (StringUtils.hasText(result.error())) {
                failureCount.incrementAndGet();
                log.warn("RAGAS item failed evaluationId={}: {}", result.evaluationId(), result.error());
                continue;
            }

            // 保存评分
            EvaluationOutcome outcome = insertScore(evaluation, runId, result);
            if (outcome == EvaluationOutcome.SUCCESS) {
                successCount.incrementAndGet();
            }
        }

        // 检查 RAGAS 是否有漏回的数据
        int missing = byId.size() - seen.size();
        if (missing > 0) {
            failureCount.addAndGet(missing);
            log.warn("RAGAS response missed {} requested items", missing);
        }
    }

    /**
     * 插入单条评分记录
     *
     * @param evaluation 原始样本
     * @param runId 任务ID
     * @param result RAGAS 评分结果
     * @return EvaluationOutcome SUCCESS 或 SKIPPED
     */
    private EvaluationOutcome insertScore(
            ChatEvaluationEntity evaluation,
            String runId,
            RagasEvaluationResult result) {
        ChatEvaluationAutoEntity entity = new ChatEvaluationAutoEntity();
        entity.setEvaluationId(evaluation.getId());
        entity.setRunId(runId);

        // 清洗 6 项指标（归一化到 [0,1] 区间）
        entity.setFaithfulness(normalizeScore(result.faithfulness()));
        entity.setAnswerRelevancy(normalizeScore(result.answerRelevancy()));
        entity.setContextPrecision(normalizeScore(result.contextPrecision()));
        entity.setContextRecall(normalizeScore(result.contextRecall()));
        entity.setAnswerCorrectness(normalizeScore(result.answerCorrectness()));
        entity.setAnswerSimilarity(normalizeScore(result.answerSimilarity()));

        // 参考答案哈希（用于去重）
        entity.setReferenceAnswerHash(referenceHash(evaluation.getReference()));

        try {
            autoMapper.insert(entity);
            return EvaluationOutcome.SUCCESS;
        } catch (DuplicateKeyException e) {
            // 唯一索引冲突：重复数据，跳过
            log.info("Skip duplicate RAGAS evaluation evaluationId={} referenceHash={}",
                    evaluation.getId(), entity.getReferenceAnswerHash());
            return EvaluationOutcome.SKIPPED;
        }
    }

    // ==================== 数据转换 ====================

    /**
     * 将数据库实体转成 RAGAS 请求格式
     */
    private RagasEvaluationItem toRagasItem(ChatEvaluationEntity evaluation) {
        return new RagasEvaluationItem(
                evaluation.getId(),
                evaluation.getQuestion(),
                evaluation.getAnswer(),
                parseRetrievedContexts(evaluation.getContextSnippets()),
                StringUtils.hasText(evaluation.getReference()) ? evaluation.getReference() : null);
    }

    /**
     * 解析数据库中的 JSON 上下文片段
     *
     * 输入：[{"text":"文档A"},{"text":"文档B"}]
     * 输出：["文档A", "文档B"]
     */
    private List<String> parseRetrievedContexts(String snippetsJson) {
        if (snippetsJson == null || snippetsJson.isBlank() || "[]".equals(snippetsJson.trim())) {
            return List.of();
        }
        try {
            List<Map<String, Object>> snippets = objectMapper.readValue(
                    snippetsJson,
                    new TypeReference<List<Map<String, Object>>>() {});
            return snippets.stream()
                    .map(snippet -> String.valueOf(snippet.getOrDefault("text", "")))
                    .filter(StringUtils::hasText)
                    .toList();
        } catch (JsonProcessingException e) {
            log.warn("Failed to parse context snippets for RAGAS", e);
            return List.of();
        }
    }

    // ==================== 分布式锁 ====================

    /**
     * 获取分布式锁
     *
     * 乐观锁风格：先尝试插入/更新锁，再确认 owner_id 是否等于自己
     *
     * @param ownerId 锁拥有者标识
     * @return true 获取成功，false 获取失败
     */
    private boolean acquireLock(String ownerId) {
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime lockedUntil = now.plusMinutes(runTimeoutMinutes);
        lockMapper.acquireOrRefreshExpired(LOCK_NAME, ownerId, lockedUntil, now);
        return ownerId.equals(lockMapper.selectOwner(LOCK_NAME));
    }

    /**
     * 释放分布式锁（只有拥有者才能释放）
     */
    private void releaseLock(String ownerId) {
        try {
            lockMapper.release(LOCK_NAME, ownerId);
        } catch (Exception e) {
            log.warn("Failed to release RAGAS auto evaluation lock ownerId={}: {}", ownerId, e.getMessage());
        }
    }

    // ==================== 任务状态管理 ====================

    /**
     * 完成任务：计算平均分并更新状态
     */
    private void completeRun(String runId, int successCount, int failureCount) {
        // 查询本轮所有样本的平均分
        Map<String, Object> averages = runMapper.selectRunAverages(runId);

        // 判断状态：有成功样本 → COMPLETED，全部失败 → FAILED
        String status = successCount > 0 || failureCount == 0 ? COMPLETED : FAILED;
        String errorMessage = successCount > 0 || failureCount == 0
                ? null
                : "All RAGAS evaluation items failed.";

        // 更新任务记录
        runMapper.updateCompletion(
                runId,
                status,
                successCount,
                failureCount,
                toScoreBigDecimal(averages.get("avg_faithfulness")),
                toScoreBigDecimal(averages.get("avg_answer_relevancy")),
                toScoreBigDecimal(averages.get("avg_context_precision")),
                toScoreBigDecimal(averages.get("avg_context_recall")),
                toScoreBigDecimal(averages.get("avg_answer_correctness")),
                toScoreBigDecimal(averages.get("avg_answer_similarity")),
                LocalDateTime.now(),
                errorMessage);
    }

    /**
     * 标记任务失败（用于异常场景）
     */
    private Mono<Void> markRunFailed(String runId, String message) {
        return Mono.fromRunnable(() -> runMapper.updateCompletion(
                        runId,
                        FAILED,
                        0,
                        1,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        LocalDateTime.now(),
                        message))
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    // ==================== 工具方法 ====================

    /** 归一化分数到 [0,1] 区间，保留4位小数 */
    private BigDecimal normalizeScore(Double score) {
        if (score == null || score.isNaN() || score.isInfinite()) {
            return null;
        }
        double clamped = Math.max(0.0, Math.min(1.0, score));
        return BigDecimal.valueOf(clamped).setScale(4, RoundingMode.HALF_UP);
    }

    /** 转为 BigDecimal，保留4位小数 */
    private BigDecimal toScoreBigDecimal(Object value) {
        if (value instanceof BigDecimal decimal) {
            return decimal.setScale(4, RoundingMode.HALF_UP);
        }
        if (value instanceof Number number) {
            return BigDecimal.valueOf(number.doubleValue()).setScale(4, RoundingMode.HALF_UP);
        }
        return null;
    }

    /** 计算参考答案的 SHA-256 哈希（用于去重） */
    private String referenceHash(String reference) {
        if (!StringUtils.hasText(reference)) {
            return NO_REFERENCE;
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(reference.getBytes(StandardCharsets.UTF_8));
            StringBuilder builder = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                builder.append(String.format("%02x", b));
            }
            return builder.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 digest is unavailable", e);
        }
    }

    /** 将实体转为 DTO */
    private AutoScoreItem toScoreItem(ChatEvaluationAutoEntity entity) {
        return new AutoScoreItem(
                entity.getId(),
                entity.getEvaluationId(),
                entity.getRunId(),
                toNullableDouble(entity.getFaithfulness()),
                toNullableDouble(entity.getAnswerRelevancy()),
                toNullableDouble(entity.getContextPrecision()),
                toNullableDouble(entity.getContextRecall()),
                toNullableDouble(entity.getAnswerCorrectness()),
                toNullableDouble(entity.getAnswerSimilarity()),
                entity.getReferenceAnswerHash(),
                entity.getCreateDate());
    }

    /** 将运行记录实体转为 DTO */
    private AutoRunItem toRunItem(ChatEvaluationAutoRunEntity entity) {
        if (entity == null) {
            return null;
        }
        return new AutoRunItem(
                entity.getRunId(),
                entity.getStatus(),
                entity.getTotalSamples(),
                entity.getSuccessCount(),
                entity.getFailureCount(),
                toNullableDouble(entity.getAvgFaithfulness()),
                toNullableDouble(entity.getAvgAnswerRelevancy()),
                toNullableDouble(entity.getAvgContextPrecision()),
                toNullableDouble(entity.getAvgContextRecall()),
                toNullableDouble(entity.getAvgAnswerCorrectness()),
                toNullableDouble(entity.getAvgAnswerSimilarity()),
                entity.getStartedAt(),
                entity.getCompletedAt(),
                entity.getErrorMessage());
    }

    /** 分批工具方法 */
    private List<List<ChatEvaluationEntity>> partition(List<ChatEvaluationEntity> items, int size) {
        if (items == null || items.isEmpty()) {
            return List.of();
        }
        List<List<ChatEvaluationEntity>> batches = new ArrayList<>();
        for (int i = 0; i < items.size(); i += size) {
            batches.add(items.subList(i, Math.min(i + size, items.size())));
        }
        return batches;
    }

    /** 生成任务ID */
    private String newRunId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /** 解析主机名作为实例ID */
    private String resolveInstanceId() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-host";
        }
    }

    /** 安全转为 long */
    private long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return 0L;
    }

    /** 安全转为 Double */
    private Double toNullableDouble(Object value) {
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        return null;
    }

    /** 评估结果枚举 */
    private enum EvaluationOutcome {
        SUCCESS,
        SKIPPED
    }
}