package net.topikachu.rag.business.document.service.impl;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.business.document.entity.AclRefreshStatus;
import net.topikachu.rag.business.document.entity.Document;
import net.topikachu.rag.business.document.entity.DocumentStatus;
import net.topikachu.rag.business.document.mapper.DocumentMapper;
import net.topikachu.rag.business.document.service.EtlJobService;
import net.topikachu.rag.business.document.vo.BatchUploadResponse;
import net.topikachu.rag.business.document.vo.UploadItemResult;
import net.topikachu.rag.business.document.vo.UploadResult;
import net.topikachu.rag.observability.TracingSupport;
import net.topikachu.rag.service.storage.ObjectStorageService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * 文档上传处理器
 * 核心能力：文件接收、安全校验、SHA256内容去重、MinIO存储、MySQL落库、ETL任务排队、异常补偿清理
 * 设计模式：上传接口立即返回，解析/分块/向量化交给后台Worker异步执行，实现上传不阻塞HTTP
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class DocumentUploadHandler {

    private final DocumentMapper documentMapper;         // 文档元数据表mapper
    private final TracingSupport tracingSupport;         // 链路追踪工具，埋点上传链路
    private final EtlJobService etlJobService;           // ETL任务服务，写入文档解析任务队列
    private final PlatformTransactionManager transactionManager; // Spring事务管理器
    private final ObjectStorageService objectStorageService; // MinIO对象存储服务，保存原始文档

    @Value("${rag.upload.max-size-bytes:52428800}")
    private long maxSizeBytes; // 上传最大文件大小，默认50MB

    @Value("${input.directory}")
    private String inputDirectory; // 服务器本地临时目录，接收上传文件临时落盘

    @Value("${rag.upload.allowed-ext:pdf,doc,docx,txt,md}")
    private String allowedExt; // 允许上传的文件扩展名白名单

    /**
     * 单文件上传入口
     * @param filePart WebFlux multipart文件对象
     * @param fileName 前端自定义文件名，可为null
     * @param overwrite 是否覆盖，当前未实现覆盖逻辑，传true遇到重复直接抛异常
     * @param userId 上传用户ID
     * @param tags 文档标签列表
     * @return UploadResult 上传结果，接口立刻返回，不等待ETL解析完成
     */
    public Mono<UploadResult> upload(FilePart filePart, String fileName, boolean overwrite, String userId, List<String> tags) {
        // 优先使用前端传入文件名，没有则取http原始文件名
        String requestedFileName = StringUtils.hasText(fileName) ? fileName : filePart.filename();
        // 获取请求content‑type，为空则默认二进制流
        String contentType = filePart.headers().getContentType() == null
                ? MediaType.APPLICATION_OCTET_STREAM_VALUE
                : filePart.headers().getContentType().toString();
        return createTempFile()
                // 将http上传流写入服务器本地临时磁盘文件
                .flatMap(tempFile -> filePart.transferTo(tempFile)
                        // 执行上传完整业务逻辑
                        .then(processUploadedFile(tempFile, requestedFileName, overwrite, userId, tags, contentType))
                        // doFinally：无论成功/失败，都删除临时文件，释放磁盘
                        .doFinally(s -> safeDelete(tempFile)));
    }

    /**
     * 批量上传接口
     * flatMapSequential：限制并发数3，防止IO风暴打满磁盘、对象存储
     * 单个文件异常不会打断整体批量任务，收集全部结果返回前端
     * @param files 文件流Flux
     * @param overwrite 是否覆盖
     * @param userId 上传用户ID
     * @param tags 批量共用标签
     * @return BatchUploadResponse 批量汇总结果，包含成功、新增、已存在、失败计数和每一条明细
     */
    public Mono<BatchUploadResponse> uploadBatch(Flux<FilePart> files, boolean overwrite, String userId, List<String> tags) {
        return files.flatMapSequential(file -> upload(file, null, overwrite, userId, tags)
                        // 上传成功，组装上传明细
                        .map(result -> UploadItemResult.builder()
                                .success(true)
                                .created(result.isCreated())
                                .docUuid(result.getDocUuid())
                                .fileName(result.getFileName())
                                .status(result.getStatus())
                                .fileHash(result.getFileHash())
                                .build())
                        // 捕获单文件异常，不向外抛出，封装失败明细，保证批量继续执行
                        .onErrorResume(error -> {
                            log.error("Batch upload failed: fileName={}, err={}", file.filename(), error.toString(), error);
                            return Mono.just(UploadItemResult.builder()
                                    .success(false)
                                    .created(false)
                                    .fileName(file.filename())
                                    .error(error.getMessage())
                                    .build());
                        }), 3)
                // 收集全部文件处理结果
                .collectList()
                .map(results -> {
                    int success = 0;
                    int created = 0;
                    int existed = 0;
                    int failed = 0;
                    for (UploadItemResult result : results) {
                        if (result.isSuccess()) {
                            success++;
                            if (result.isCreated()) {
                                created++;
                            } else {
                                existed++;
                            }
                        } else {
                            failed++;
                        }
                    }
                    return BatchUploadResponse.builder()
                            .total(results.size())
                            .successCount(success)
                            .createdCount(created)
                            .existedCount(existed)
                            .failedCount(failed)
                            .results(results)
                            .build();
                });
    }

    /**
     * 创建本地临时文件
     * IO阻塞操作，调度到boundedElastic线程池，不占用WebFlux事件循环线程
     * @return 临时文件Path对象
     */
    private Mono<Path> createTempFile() {
        return Mono.fromCallable(() -> {
                    Path baseDir = Paths.get(inputDirectory).toAbsolutePath().normalize();
                    Files.createDirectories(baseDir);
                    return Files.createTempFile(baseDir, "upload_", ".tmp");
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 处理已经落盘的临时文件：链路追踪 → 文件校验净化 → 计算SHA256哈希 → 查重持久化
     * @param tempFile 本地临时文件路径
     * @param fileName 请求的原始文件名
     * @param overwrite 是否覆盖
     * @param userId 用户ID
     * @param tags 标签列表
     * @param contentType http请求内容类型
     * @return 上传结果Mono
     */
    private Mono<UploadResult> processUploadedFile(Path tempFile,
                                                   String fileName,
                                                   boolean overwrite,
                                                   String userId,
                                                   List<String> tags,
                                                   String contentType) {
        return tracingSupport.traceMono("etl.upload_accept",
                uploadTraceTags(fileName, null, userId, tags),
                validateAndSanitize(tempFile, fileName)
                        .flatMap(finalFileName -> computeHash(tempFile)
                                .flatMap(hash -> persistOrDedupe(tempFile, finalFileName, hash, overwrite, userId, tags, contentType))));
    }

    /**
     * 去重与持久化主流程
     * 1、通过SHA256哈希查询数据库判断文件内容是否已经上传
     * 2、查到重复文档 → 进入handleExistingDuplicate
     * 3、查不到文档 → 执行createNewDocument新建文档
     * @param tempFile 临时文件
     * @param finalFileName 净化后的文件名
     * @param hash 文件SHA256哈希
     * @param overwrite 是否覆盖
     * @param userId 用户ID
     * @param tags 标签
     * @param contentType 文件类型
     * @return UploadResult
     */
    private Mono<UploadResult> persistOrDedupe(Path tempFile, String finalFileName, String hash,
                                               boolean overwrite, String userId, List<String> tags,
                                               String contentType) {
        return lookupExistingDocument(hash)
                .flatMap(existing -> handleExistingDuplicate(existing, overwrite))
                .switchIfEmpty(Mono.defer(() ->
                        createNewDocument(tempFile, finalFileName, hash, userId, tags, contentType)));
    }

    /**
     * 文件校验 + 文件名净化
     * @param tempFile 临时文件路径
     * @param fileName 原始文件名
     * @return 净化之后的文件名
     */
    private Mono<String> validateAndSanitize(Path tempFile, String fileName) {
        return Mono.fromCallable(() -> {
                    validateFile(tempFile, fileName); // 校验大小、扩展名、文件有效性
                    return sanitizeFileName(fileName); // 净化文件名，防御路径穿越攻击
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 计算文件SHA‑256哈希值，作为文件内容指纹，用于内容去重
     * @param tempFile 临时文件路径
     * @return sha256十六进制字符串
     */
    private Mono<String> computeHash(Path tempFile) {
        return Mono.fromCallable(() -> sha256(tempFile))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 根据文件hash查询数据库，查找已经存在的文档记录
     * @param hash sha256文件哈希
     * @return Document，查不到返回null
     */
    private Mono<Document> lookupExistingDocument(String hash) {
        return Mono.fromCallable(() -> findByHash(hash))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 处理重复文件逻辑
     * overwrite=true：直接抛出异常，当前没有实现覆盖逻辑
     * overwrite=false：直接返回旧文档记录，不上传存储，不新增数据库记录
     * @param existing 数据库中已存在文档
     * @param overwrite 是否覆盖
     * @return UploadResult
     */
    private Mono<UploadResult> handleExistingDuplicate(Document existing, boolean overwrite) {
        if (overwrite) {
            return Mono.error(new IllegalArgumentException(
                    "The file already exists : " + existing.getFileName()));
        }
        // created=false：代表本次没有新建文档，复用历史记录
        return Mono.just(toResult(existing, false));
    }

    /**
     * 创建全新文档：生成docUuid、objectKey，组装实体，调用存储与入库逻辑
     * @param tempFile 临时文件
     * @param finalFileName 净化后文件名
     * @param hash 文件sha256哈希
     * @param userId 用户ID
     * @param tags 标签
     * @param contentType 文件类型
     * @return UploadResult
     */
    private Mono<UploadResult> createNewDocument(Path tempFile, String finalFileName, String hash,
                                                 String userId, List<String> tags, String contentType) {
        String docUuid = generateDocUuid(); // 生成32位无横杠UUID，对外暴露的文档唯一ID，不使用自增主键
        // MinIO对象存储路径规范 documents/{docUuid}/{fileName}
        String objectKey = "documents/" + docUuid + "/" + finalFileName;
        Document doc = buildNewDocument(docUuid, finalFileName, hash, tags, objectKey);
        return uploadToStorageAndPersist(tempFile, objectKey, contentType, doc, userId)
                .thenReturn(toResult(doc, true));
    }

    /**
     * 构建新文档数据库实体，设置默认字段
     * 默认权限：spaceCode=public、isPublic=true，文档默认全部用户可读；aclVersion初始=1
     * status=UPLOADED：文件上传完成，但尚未执行解析、分块、向量化
     * @param docUuid 文档对外唯一ID
     * @param fileName 文件名
     * @param hash 文件sha256哈希
     * @param tags 标签列表
     * @param objectKey MinIO存储路径
     * @return Document实体
     */
    private Document buildNewDocument(String docUuid, String fileName, String hash,
                                      List<String> tags, String objectKey) {
        Document doc = new Document();
        doc.setDocUuid(docUuid);
        doc.setFileName(fileName);
        doc.setStatus(DocumentStatus.UPLOADED.name());
        doc.setFileHash(hash);
        doc.setTags(tags);
        doc.setSpaceCode("public");
        doc.setIsPublic(Boolean.TRUE);
        doc.setOwnerDeptId(null);
        doc.setAllowedRoles(null);
        doc.setAllowedDeptIds(null);
        doc.setAclVersion(1);
        doc.setAclRefreshStatus(AclRefreshStatus.PENDING.name());
        doc.setAclRefreshError(null);
        doc.setAclRefreshTime(null);
        doc.setCreateDate(LocalDateTime.now());
        doc.setUpdateDate(LocalDateTime.now());
        doc.setObjectKey(objectKey);
        return doc;
    }

    /**
     * 上传MinIO对象存储 + 异常补偿 + 调用事务入库与ETL入队
     * 执行顺序：先上传MinIO，再执行数据库事务
     * 补偿逻辑：MinIO上传成功，后续事务/业务报错，则删除MinIO已经上传的对象，避免产生孤儿垃圾文件
     * @param tempFile 本地临时文件
     * @param objectKey MinIO存储key
     * @param contentType 文件类型
     * @param doc 文档实体
     * @param userId 用户ID
     * @return Mono<Void>
     */
    private Mono<Void> uploadToStorageAndPersist(Path tempFile, String objectKey, String contentType,
                                                 Document doc, String userId) {
        AtomicBoolean objectUploaded = new AtomicBoolean(false); // 标记是否已经成功上传MinIO，用于异常补偿
        return objectStorageService.putObject(objectKey, tempFile, contentType)
                .doOnSuccess(v -> objectUploaded.set(true))
                .then(Mono.defer(() -> persistDocumentAndQueueEtl(doc, objectKey, userId)))
                .doOnSuccess(v -> log.info("Upload created: docUuid={}, fileName={}, status={}, fileHash={}, path={}",
                        doc.getDocUuid(), doc.getFileName(), doc.getStatus(), doc.getFileHash(), objectKey))
                // 异常兜底：如果MinIO上传成功，后面逻辑出错，清理MinIO对象，再向上抛出异常
                .onErrorResume(e -> {
                    Mono<Void> cleanup = objectUploaded.get()
                            ? objectStorageService.deleteObject(objectKey)
                            : Mono.empty();
                    return cleanup.then(Mono.error(e));
                });
    }

    /**
     * 数据库事务：插入document记录，同步写入ETL任务队列记录
     * 注意：queueDocumentIngestionSync名字带Sync，不是同步执行解析，只是同步写任务记录；真正解析交给后台Worker异步消费
     * insert文档 和 queueDocumentIngestionSync 在同一个事务，要么全部成功，要么全部回滚
     * @param doc 文档实体
     * @param objectKey MinIO路径
     * @param userId 用户ID
     * @return Mono<Void>
     */
    private Mono<Void> persistDocumentAndQueueEtl(Document doc, String objectKey, String userId) {
        return Mono.fromCallable(() -> {
            TransactionTemplate tx = new TransactionTemplate(transactionManager);
            tx.executeWithoutResult(status -> {
                int inserted = documentMapper.insert(doc);
                if (inserted != 1) {
                    throw new IllegalStateException("Insert document failed");
                }
                // 将ETL任务写入数据库任务表，后台worker消费执行文档解析、分块、embedding
                etlJobService.queueDocumentIngestionSync(doc, objectKey, userId);
            });
            return null;
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    /**
     * 组装链路追踪标签，把上传关键信息埋入Trace，方便线上排查上传问题
     */
    private Map<String, Object> uploadTraceTags(String fileName, String docUuid, String userId, List<String> tags) {
        Map<String, Object> traceTags = new LinkedHashMap<>();
        traceTags.put("document.file_name", fileName);
        traceTags.put("document.doc_uuid", docUuid);
        traceTags.put("document.user_id", userId);
        traceTags.put("document.tags", tags == null ? "" : String.join(",", tags));
        return traceTags;
    }

    /**
     * 组装返回给前端的上传结果对象
     * @param doc 文档实体
     * @param created true本次新建；false文件重复，复用旧记录
     * @return UploadResult
     */
    private UploadResult toResult(Document doc, boolean created) {
        return UploadResult.builder()
                .created(created)
                .docUuid(doc.getDocUuid())
                .fileName(doc.getFileName())
                .status(doc.getStatus())
                .fileHash(doc.getFileHash())
                .build();
    }

    /**
     * 根据sha256哈希查询文档，实现按文件内容去重
     * @param hash 文件sha256哈希
     * @return Document，查不到返回null
     */
    private Document findByHash(String hash) {
        if (!StringUtils.hasText(hash)) {
            return null;
        }
        return documentMapper.selectOne(Wrappers.<Document>lambdaQuery()
                .eq(Document::getFileHash, hash)
                .last("LIMIT 1"));
    }

    /**
     * 文件校验：文件存在性、文件大小上限、文件名非空、扩展名白名单校验
     * @param path 临时文件路径
     * @param fileName 请求文件名
     * @throws IOException 校验失败抛出业务异常拒绝上传
     */
    private void validateFile(Path path, String fileName) throws IOException {
        if (path == null || !Files.exists(path)) {
            throw new IllegalArgumentException("File is empty.");
        }
        if (Files.size(path) > maxSizeBytes) {
            throw new IllegalArgumentException("File too large. max=" + maxSizeBytes + " bytes");
        }

        String name = StringUtils.hasText(fileName) ? fileName : path.getFileName().toString();
        name = name == null ? "" : name.trim();
        if (!StringUtils.hasText(name)) {
            throw new IllegalArgumentException("File name is blank.");
        }

        String ext = getExtension(name);
        Set<String> allow = Arrays.stream(allowedExt.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(String::toLowerCase)
                .collect(Collectors.toSet());

        if (ext.isEmpty() || !allow.contains(ext)) {
            throw new IllegalArgumentException("File extension not allowed: " + ext + ", allowed=" + allow);
        }
    }

    /**
     * 生成docUuid，32位无"-"的UUID，文档对外唯一标识，避免暴露数据库自增主键
     * @return 32位字符串
     */
    private String generateDocUuid() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 文件名净化，防御路径穿越攻击
     * 1、只保留文件名部分，丢弃上级路径
     * 2、替换 \ / : * ? " < > | 等危险特殊字符为下划线
     * @param name 原始文件名
     * @return 安全的文件名
     */
    private String sanitizeFileName(String name) {
        String normalized = name.trim();
        normalized = Paths.get(normalized).getFileName().toString();
        normalized = normalized.replaceAll("[\\\\/:*?\"<>|]", "_");
        if (!StringUtils.hasText(normalized)) {
            throw new IllegalArgumentException("Invalid file name after sanitize.");
        }
        return normalized;
    }

    /**
     * 获取文件扩展名，转小写
     * @param filename 文件名
     * @return 小写扩展名，无扩展名返回空字符串
     */
    private String getExtension(String filename) {
        int idx = filename.lastIndexOf('.');
        if (idx < 0 || idx == filename.length() - 1) {
            return "";
        }
        return filename.substring(idx + 1).toLowerCase(Locale.ROOT);
    }

    /**
     * 静默删除本地临时文件，吃掉异常；无论成功失败都要清理临时文件释放磁盘
     * @param path 临时文件路径
     */
    private void safeDelete(Path path) {
        if (path == null) {
            return;
        }
        try {
            Files.deleteIfExists(path);
        } catch (Exception ignore) {
        }
    }

    /**
     * 计算文件SHA‑256摘要
     * DigestInputStream边读文件边计算哈希，读完整个文件得到摘要，输出十六进制字符串
     * @param path 文件路径
     * @return sha256十六进制字符串
     */
    private String sha256(Path path) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (InputStream in = Files.newInputStream(path);
                 DigestInputStream dis = new DigestInputStream(in, digest)) {
                // 把全部文件字节读入DigestInputStream，输出到空流，完成哈希计算
                dis.transferTo(java.io.OutputStream.nullOutputStream());
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (Exception e) {
            throw new RuntimeException("Failed to calculate hash for " + path, e);
        }
    }
}
