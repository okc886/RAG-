package net.topikachu.rag.service.etl;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.business.document.entity.Document;
import net.topikachu.rag.business.document.entity.EtlJob;
import net.topikachu.rag.business.document.entity.EtlJobStatus;
import net.topikachu.rag.business.document.mapper.DocumentMapper;
import net.topikachu.rag.business.document.mapper.EtlJobMapper;
import net.topikachu.rag.business.document.service.EtlJobService;
import net.topikachu.rag.business.document.service.impl.EtlJobLeaseService;
import net.topikachu.rag.service.storage.ObjectStorageService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/**
 * ETL任务工作器
 * 定时轮询数据库拉取待执行文档导入任务，基于数据库租约实现分布式任务抢占
 * 支持并发处理、长任务心跳续租、对象存储文件下载、临时文件清理、失败指数退避重试
 * Reactor响应式编程，IO操作全部放到boundedElastic线程池，不阻塞reactor事件线程
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class EtlJobWorker {

    private final DocumentMapper documentMapper;            // 文档主表Mapper
    private final EtlPipeline etlPipeline;                  // ETL核心流水线：文件解析、切分chunk、向量化、入库
    @Value("${rag.etl.worker.batch-size:10}")
    private int batchsize;                                  // 每一次轮询最多拉取多少个待运行任务
    @Value("${rag.etl.worker.lock-minutes:10}")
    private int lockMinutes;                                // 任务租约锁有效期，单位分钟
    @Value("${rag.etl.worker.concurrency:4}")
    private int concurrency;                                // 同时并发执行多少个ETL任务
    private final EtlJobLeaseService etlJobLeaseService;    // 任务租约续租服务
    private final EtlJobService etlJobService;              // EtlJob业务服务：查询可运行任务、抢占任务锁
    private final EtlJobMapper etlJobMapper;                // ETL任务数据库Mapper
    private final ObjectStorageService objectStorageService; // 对象存储服务，用于下载文档到本地临时文件

    // 当前worker实例唯一标识，每个服务实例启动生成一次，去掉UUID连字符，用于分布式租约标记locked_by
    private final String workerId = UUID.randomUUID().toString().replace("-","");

    /**
     * 定时轮询入口
     * fixedDelayString：上一轮执行完成之后，间隔5秒再执行下一轮，不是固定频率
     * 捕获顶层异常，防止定时线程因为异常直接死亡
     */
    @Scheduled(fixedDelayString = "${rag.etl.worker.fixed-delay-ms:5000}")
    public void poll() {
        pollOnce().subscribe(null, error -> log.error("ETL worker poll failed", error));
    }

    /**
     * 执行一次轮询：查询一批可运行任务，交给processJobs并发处理
     */
    Mono<Void> pollOnce() {
        return etlJobService.findRunnableJobs(batchsize)
                .flatMap(this::processJobs);
    }

    /**
     * 批量处理一批ETL任务，限制并发数
     * @param jobs 待执行任务列表
     * @return Mono<Void> 可组合的响应式对象，单元测试可以block()等待全部任务完成
     */
    Mono<Void> processJobs(List<EtlJob> jobs) {
        if (jobs == null || jobs.isEmpty()) {
            return Mono.empty();
        }
        // 将List转为Flux流，flatMap设置最大并发，then丢弃元素只保留完成信号
        return Flux.fromIterable(jobs)
                .flatMap(this::tryClaimAndRun, Math.max(1, concurrency))
                .doOnError(error -> log.error("ETL worker poll batch failed", error))
                .then();
    }

    /**
     * 尝试抢占任务租约，抢占成功才真正运行任务
     * 数据库CAS抢占锁：标记status=RUNNING、locked_by=workerId、locked_until租约到期时间
     */
    private Mono<Void> tryClaimAndRun(EtlJob job) {
        if(job == null){
            log.warn("Document is empty");
            return Mono.empty();
        }

        LocalDateTime now = LocalDateTime.now();
        // markRunning：数据库CAS抢占任务锁，返回true代表抢占成功
        return etlJobService.markRunning(job.getId(), workerId, now.plusMinutes(lockMinutes))
                .flatMap(claimed -> {
                    if (claimed) {
                        // 抢占锁成功，执行任务
                        return runJob(job.getId());
                    }
                    // 抢占失败：任务已经被其他worker实例拿走，直接跳过
                    log.warn("Failed to update Document status : {}", job.getId());
                    return Mono.empty();
                })
                .onErrorResume(error -> {
                    log.error("Failed to claim or run ETL job: {}", job.getId(), error);
                    return Mono.empty();
                });
    }

    /**
     * 根据jobId重新查询数据库EtlJob记录，交给runJobInternal执行业务
     * DB阻塞IO放到boundedElastic线程池，不能跑在reactor事件线程
     */
    private Mono<Void> runJob(String jobId) {
        return Mono.fromCallable(() -> etlJobMapper.selectById(jobId))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(this::runJobInternal);
    }

    /**
     * 任务内部执行主流程
     * 1.开启心跳续租；2.准备本地文件；3.执行ETL导入；4.捕获异常处理失败；5.无论成败关闭心跳释放资源
     */
    private Mono<Void> runJobInternal(EtlJob job) {
        if (job == null) {
            log.warn("Not found job worker");
            return Mono.empty();
        }

        // 开启心跳定时续租，拿到Disposable句柄用于后续关闭定时任务，防止内存泄漏
        Disposable heartbeat = startHeartbeat(job.getId());
        return prepareJobFile(job)
                .flatMap(jobFile -> runIngestion(job, jobFile))
                // 捕获runIngestion抛出的所有异常，进入失败处理逻辑
                .onErrorResume(error -> handleFailure(job, error))
                // 无论成功、异常、取消，都关闭心跳定时任务
                .doFinally(signalType -> heartbeat.dispose());
    }

    /**
     * 执行ETL导入流水线
     * 1.调用etlPipeline执行文档解析、切分、向量化入库
     * 2.上游正常完成后执行markSuccess标记任务数据库状态成功
     * 3.doFinally：无论成功失败，清理对象存储下载产生的临时文件
     */
    private Mono<Void> runIngestion(EtlJob job, JobFile jobFile) {
        return etlPipeline.ingestionByPath(
                        jobFile.path(),
                        job.getDocUuid(),
                        job.getCreateUserId(),
                        job.getTags())
                // 上游ingestionByPath发出onComplete才执行markSuccess；如果上游error则跳过then
                .then(markSuccess(job))
                .doFinally(signalType -> {
                    // tempDownloaded=true代表是对象存储下载下来的临时文件，需要删除；本地源文件不能删除
                    if (jobFile.tempDownloaded()) {
                        // 裸subscribe独立订阅，删除文件异常不影响主ETL任务流程，不等待删除完成
                        deleteTempFileWithRetry(jobFile.path()).subscribe();
                    }
                });
    }

    /**
     * 任务失败处理入口，记录日志，调用markFailed更新数据库失败状态、异常堆栈、重试时间
     */
    private Mono<Void> handleFailure(EtlJob job, Throwable error) {
        log.info("Failed to execute the task: {}", job.getDocUuid());
        return markFailed(job, error);
    }

    /**
     * 准备待解析的文件
     * 分支1：对象存储存在objectKey → 下载到本地临时文件 tempDownloaded=true
     * 分支2：本地文件路径 → 直接使用磁盘源文件 tempDownloaded=false
     */
    private Mono<JobFile> prepareJobFile(EtlJob job) {
        return Mono.fromCallable(() -> {
                    // 根据docUuid查询Document文档记录
                    Document doc = documentMapper.selectOne(
                            Wrappers.<Document>lambdaQuery().eq(Document::getDocUuid, job.getDocUuid()).last("LIMIT 1")
                    );
                    if (doc == null) {
                        throw new IllegalStateException("Document not found: " + job.getDocUuid());
                    }
                    return doc;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(doc -> {
                    // 对象存储模式：下载文件到本机临时目录
                    if (StringUtils.hasText(job.getObjectKey())) {
                        return objectStorageService.downloadToTempFile(job.getObjectKey(), job.getFileName())
                                .map(path -> new JobFile(path, true));
                    }
                    // 本地文件模式
                    Path path = Paths.get(job.getFilePath());
                    if (!Files.exists(path)) {
                        return Mono.error(new IllegalStateException("Source file missing: " + path));
                    }
                    return Mono.just(new JobFile(path, false));
                });
    }

    /**
     * JobFile 记录：封装文件路径 + 是否为下载得到的临时文件标记
     * @param path 文件本地路径
     * @param tempDownloaded true=对象存储下载临时文件，任务结束需要删除；false=原始本地文件，禁止删除
     */
    private record JobFile(Path path, boolean tempDownloaded) {
    }

    /**
     * 带重试删除临时文件入口
     */
    private Mono<Void> deleteTempFileWithRetry(Path path) {
        return deleteTempFileAttempt(path, 1)
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 重试删除临时文件
     * 文件句柄未释放会出现IO异常，最多重试5次；5次失败注册JVM退出时删除兜底
     * 每次重试间隔随attempt指数增加：200ms、400ms、600ms...
     */
    private Mono<Void> deleteTempFileAttempt(Path path, int attempt) {
        return Mono.fromCallable(() -> Files.deleteIfExists(path))
                .subscribeOn(Schedulers.boundedElastic())
                .then()
                .onErrorResume(error -> {
                    // 达到最大重试次数，放弃本次删除，JVM进程退出时尝试清理
                    if (attempt >= 5) {
                        path.toFile().deleteOnExit();
                        log.warn("Failed to delete ETL temp file after {} attempts, scheduled deleteOnExit: {}",
                                5, path, error);
                        return Mono.empty();
                    }
                    // 延时后递归重试
                    return Mono.delay(Duration.ofMillis(200L * attempt))
                            .then(deleteTempFileAttempt(path, attempt + 1));
                });
    }

    /**
     * 开启任务租约心跳续租
     * 长任务超过lockMinutes租约时间，需要定时向后延期locked_until，防止被其他worker抢占
     * intervalSeconds = lockMinutes/3，保证租约到期前至少续约3次，最小间隔30秒，防止数据库压力过大
     * onErrorContinue：心跳自身异常不要终止整个ETL任务，仅打印警告日志
     */
    private Disposable startHeartbeat(String jobId) {
        long intervalSeconds = Math.max(30, lockMinutes * 60L / 3);

        return Flux.interval(Duration.ofSeconds(intervalSeconds))
                .flatMap(tick -> Mono.fromCallable(() ->
                                etlJobLeaseService.renewLease(
                                        jobId,
                                        workerId,
                                        LocalDateTime.now().plusMinutes(lockMinutes)))
                        .subscribeOn(Schedulers.boundedElastic()))
                .doOnNext(renewed -> {
                    // renewed=false：租约丢失，说明任务已经被其他worker抢占
                    if (!renewed) {
                        log.warn("ETL job lease heartbeat lost: jobId={}, workerId={}", jobId, workerId);
                    }
                })
                .onErrorContinue((error, value) ->
                        log.warn("ETL job lease heartbeat failed: jobId={}, workerId={}", jobId, workerId, error))
                .subscribe();
    }

    /**
     * CAS更新任务状态为SUCCESS
     * where条件必须同时满足 id、status=RUNNING、locked_by=当前workerId
     * 如果租约丢失，数据库受影响行数!=1，放弃更新状态，打印警告日志
     */
    private Mono<Void> markSuccess(EtlJob job){
        return Mono.fromRunnable(() -> {
                    LocalDateTime now = LocalDateTime.now();
                    int updated = etlJobMapper.update(null,
                            Wrappers.<EtlJob>lambdaUpdate()
                                    .set(EtlJob::getStatus, EtlJobStatus.SUCCESS.name())
                                    .set(EtlJob::getLockedBy, null)          // 清空租约归属worker
                                    .set(EtlJob::getLockedUntil, null)       // 清空租约到期时间
                                    .set(EtlJob::getLastError, null)         // 清空错误信息
                                    .set(EtlJob::getErrorStack, null)        // 清空异常堆栈
                                    .set(EtlJob::getUpdateDate, now)
                                    .set(EtlJob::getFinishedAt, now)         // 任务完成时间
                                    .set(EtlJob::getActiveKey, null)
                                    .eq(EtlJob::getId, job.getId())
                                    .eq(EtlJob::getStatus, EtlJobStatus.RUNNING.name())
                                    .eq(EtlJob::getLockedBy, workerId)
                    );
                    // CAS校验失败：租约已经不属于当前worker，不修改状态
                    if (updated != 1) {
                        log.warn("Skip marking ETL job success because lease was lost: jobId={}, workerId={}",
                                job.getId(), workerId);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    /**
     * CAS更新任务状态为FAILED
     * 记录异常信息、堆栈；重试计数+1；计算指数退避的nextRetryTime下次重试时间
     * CAS条件：id、status=RUNNING、locked_by=当前workerId，租约丢失则放弃更新
     */
    private Mono<Void> markFailed(EtlJob job, Throwable error){
        return Mono.fromRunnable(() -> {
                    LocalDateTime now = LocalDateTime.now();

                    int retryCount = job.getRetryCount() == null ? 0 : job.getRetryCount();
                    int nextRetryCount = retryCount + 1;

                    int updated = etlJobMapper.update(null,
                            Wrappers.<EtlJob>lambdaUpdate()
                                    .set(EtlJob::getFinishedAt, now)
                                    .set(EtlJob::getStatus, EtlJobStatus.FAILED.name())
                                    .set(EtlJob::getLockedBy, null)
                                    .set(EtlJob::getLockedUntil, null)
                                    .set(EtlJob::getRetryCount, nextRetryCount)          // 重试次数+1
                                    .set(EtlJob::getUpdateDate, now)
                                    .set(EtlJob::getNextRetryTime, calculateNextRetryTime(nextRetryCount)) // 计算下一次执行时间
                                    .set(EtlJob::getLastError, summarizeError(error))    // 简短错误信息
                                    .set(EtlJob::getErrorStack, formatStackTrace(error))  // 完整异常堆栈
                                    .set(EtlJob::getActiveKey, null)
                                    .eq(EtlJob::getId, job.getId())
                                    .eq(EtlJob::getStatus, EtlJobStatus.RUNNING.name())
                                    .eq(EtlJob::getLockedBy, workerId)
                    );
                    if (updated != 1) {
                        log.warn("Skip marking ETL job failed because lease was lost: jobId={}, workerId={}",
                                job.getId(), workerId);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    /**
     * 指数退避策略，根据当前重试次数计算下一次重试时间
     * 第1次失败：5分钟后重试
     * 第2次失败：15分钟后重试
     * 第3次失败：60分钟后重试
     * 4次及以后：每360分钟(6小时)重试一次
     * 目的：防止故障场景无限疯狂重试压垮存储与向量库
     */
    private LocalDateTime calculateNextRetryTime(int retryCount) {
        int minutes = switch (retryCount) {
            case 1 -> 5;
            case 2 -> 15;
            case 3 -> 60;
            default -> 360;
        };

        return LocalDateTime.now().plusMinutes(minutes);
    }

    /**
     * 提取根异常简短错误信息，截断到1000字符适配数据库字段长度
     */
    private String summarizeError(Throwable error) {
        if (error == null) {
            return "Unknown error";
        }

        // 循环拿到最底层根cause异常
        Throwable root = error;
        while (root.getCause() != null) {
            root = root.getCause();
        }

        String message = root.getMessage();
        if (message == null || message.isBlank()) {
            message = root.getClass().getSimpleName();
        }

        return message.length() > 1000 ? message.substring(0, 1000) : message;
    }

    /**
     * 获取完整异常堆栈字符串，截断2000字符存入数据库error_stack字段
     */
    private String formatStackTrace(Throwable error) {
        if (error == null) {
            return null;
        }

        StringWriter sw = new StringWriter();
        error.printStackTrace(new PrintWriter(sw));

        String stack = sw.toString();
        return stack.length() > 2000 ? stack.substring(0, 2000) : stack;
    }
}
