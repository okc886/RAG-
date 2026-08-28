package net.topikachu.rag.business.document.service.impl;

import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.auth.CurrentUserContext;
import net.topikachu.rag.auth.SearchScope;
import net.topikachu.rag.business.document.entity.AclRefreshStatus;
import net.topikachu.rag.business.document.entity.Document;
import net.topikachu.rag.business.document.entity.DocumentStatus;
import net.topikachu.rag.business.document.entity.KnowledgeAclRefreshTask;
import net.topikachu.rag.business.document.mapper.DocumentMapper;
import net.topikachu.rag.business.document.mapper.KnowledgeAclRefreshTaskMapper;
import net.topikachu.rag.business.document.service.DocumentService;
import net.topikachu.rag.business.document.service.EtlJobService;
import net.topikachu.rag.business.document.vo.BatchUploadResponse;
import net.topikachu.rag.business.document.vo.DocumentPermissionUpdateRequest;
import net.topikachu.rag.business.document.vo.DownloadedDocument;
import net.topikachu.rag.business.document.vo.UploadResult;
import net.topikachu.rag.service.etl.KnowledgeParentBlockService;
import net.topikachu.rag.service.etl.MilvusWriteGateway;
import net.topikachu.rag.service.storage.ObjectStorageService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 文档服务实现类，WebFlux响应式编程
 * 负责文档上传、删除、下载、预览、权限更新、Etl重试、搜索范围权限校验等业务
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class DocumentServiceImpl implements DocumentService {

    // MyBatis‑Plus文档数据库Mapper
    private final DocumentMapper documentMapper;
    // Milvus向量库写入网关，负责向量库增删
    private final MilvusWriteGateway milvusWriteGateway;
    // ACL刷新任务Mapper
    private final KnowledgeAclRefreshTaskMapper aclRefreshTaskMapper;
    // ETl任务服务，文档解析分块向量化任务
    private final EtlJobService etlJobService;
    // 文档分块父块数据库操作服务
    private final KnowledgeParentBlockService parentBlockService;
    // 对象存储服务，MinIO/OSS，保存原始文档
    private final ObjectStorageService objectStorageService;
    // 文档上传处理器，处理文件接收、临时存储、校验、查重
    private final DocumentUploadHandler documentUploadHandler;
    // 文档权限管理器，判断用户是否可以访问某篇文档，组装权限SQL条件
    private final DocumentPermissionManager documentPermissionManager;
    // ACL元数据刷新管理器，文档权限变更后刷新Milvus内的ACL元数据
    private final AclRefreshManager aclRefreshManager;

    // 本地临时文件目录配置
    @Value("${input.directory}")
    private String inputDirectory;

    /**
     * 单文件上传
     * @param filePart webflux文件对象
     * @param fileName 自定义文件名
     * @param overwrite 是否覆盖已存在文档
     * @param userId 操作人id
     * @param tags 文档标签
     * @return UploadResult 上传结果vo
     */
    @Override
    public Mono<UploadResult> upload(FilePart filePart, String fileName, boolean overwrite, String userId, List<String> tags) {
        return documentUploadHandler.upload(filePart, fileName, overwrite, userId, tags);
    }

    /**
     * 批量文件上传
     * @param files 文件流Flux
     * @param overwrite 是否覆盖
     * @param userId 操作人
     * @param tags 批量共用标签
     * @return BatchUploadResponse批量上传结果
     */
    @Override
    public Mono<BatchUploadResponse> uploadBatch(Flux<FilePart> files, boolean overwrite, String userId, List<String> tags) {
        return documentUploadHandler.uploadBatch(files, overwrite, userId, tags);
    }

    /**
     * 分页查询文档列表，管理员后台使用
     * @param page 页码，从1开始
     * @param size 每页条数
     * @param keyword 文件名称模糊搜索关键词
     * @return Page<Document>分页对象
     */
    @Override
    public Mono<Page<Document>> listDocuments(int page, int size, String keyword) {
        // fromCallable：包装阻塞数据库操作；boundedElastic调度器执行阻塞IO
        return Mono.fromCallable(() -> {
                    Page<Document> resultPage = new Page<>(page, size);
                    LambdaQueryWrapper<Document> query = Wrappers.lambdaQuery();
                    if (StringUtils.hasText(keyword)) {
                        query.like(Document::getFileName, keyword);
                    }
                    query.orderByDesc(Document::getCreateDate);
                    return documentMapper.selectPage(resultPage, query);
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 根据数据库主键删除单个文档
     * 删除链路：Milvus向量库 → MySQL分块记录 → 对象存储源文件 → ACL任务记录 → MySQL文档主表
     * @param id 文档数据库主键id
     */
    @Override
    public Mono<Void> removeDocumentById(String id) {
        return Mono.fromCallable(() -> documentMapper.selectById(id))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(doc -> {
                    if (doc == null) {
                        return Mono.empty();
                    }
                    // 删除Milvus向量数据
                    return milvusWriteGateway.deleteByDocUuid(doc.getDocUuid())
                            .doOnSuccess(v -> log.info("Deleted from Milvus: docUuid={}", doc.getDocUuid()))
                            // 删除知识库分块记录
                            .then(parentBlockService.deleteByDocUuid(doc.getDocUuid()))
                            // 删除原始源文件
                            .then(deleteSourceFile(doc))
                            // 删除ACL刷新任务记录，删除文档主表记录
                            .then(Mono.fromRunnable(() -> {
                                aclRefreshTaskMapper.delete(Wrappers.<KnowledgeAclRefreshTask>lambdaQuery()
                                        .eq(KnowledgeAclRefreshTask::getDocUuid, doc.getDocUuid()));
                                documentMapper.deleteById(id);
                                log.info("Deleted from database: id={}", id);
                            }).subscribeOn(Schedulers.boundedElastic()));
                })
                .then();
    }

    /**
     * 删除文档对应的原始源文件；优先删对象存储，兜底删本地磁盘临时目录
     * @param doc 文档实体
     */
    private Mono<Void> deleteSourceFile(Document doc) {
        // objectKey不为空代表文件保存在对象存储
        if (StringUtils.hasText(doc.getObjectKey())) {
            return objectStorageService.deleteObject(doc.getObjectKey())
                    .doOnSuccess(v -> log.info("Deleted object storage file: objectKey={}", doc.getObjectKey()));
        }
        // 对象存储为空，则删除本地磁盘目录
        return Mono.fromRunnable(() -> {
                    Path baseDir = Paths.get(inputDirectory).toAbsolutePath().normalize();
                    Path dir = baseDir.resolve(doc.getDocUuid()).normalize();
                    if (Files.exists(dir)) {
                        // walk遍历目录，reverseOrder先删文件再删文件夹
                        try (var walk = Files.walk(dir)) {
                            walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                                try {
                                    Files.delete(path);
                                } catch (IOException ignore) {
                                }
                            });
                        } catch (IOException e) {
                            throw new RuntimeException("Failed to delete file directory " + dir, e);
                        }
                        log.info("Deleted file directory: {}", dir);
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    /**
     * 批量删除文档
     * @param ids 文档主键id集合
     */
    @Override
    public Mono<Void> removeDocumentsBatch(List<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return Mono.empty();
        }
        // concatMap串行逐个删除，防止IO打满
        return Flux.fromIterable(ids)
                .concatMap(this::removeDocumentById)
                .then();
    }

    /**
     * 根据主键下载文档，返回二进制流用于浏览器下载
     * @param id 文档数据库主键
     * @return DownloadedDocument 包含文件名和输入流
     */
    @Override
    public Mono<DownloadedDocument> downloadDocumentById(String id) {
        return Mono.fromCallable(() -> {
                    Document doc = documentMapper.selectById(id);
                    if (doc == null) {
                        throw new IllegalArgumentException("Document not found: " + id);
                    }
                    if (!StringUtils.hasText(doc.getObjectKey())) {
                        throw new IllegalStateException("Source file objectKey is missing");
                    }
                    return doc;
                })
                .subscribeOn(Schedulers.boundedElastic())
                // usingWhen：响应式资源管理，保证输入流一定会关闭，防止文件句柄泄漏
                .flatMap(doc -> Mono.usingWhen(
                        objectStorageService.getObject(doc.getObjectKey()),
                        is -> Mono.just(new DownloadedDocument(doc.getFileName(), is)),
                        is -> Mono.empty(),
                        (is, err) -> closeQuietly(is),
                        this::closeQuietly
                ));
    }

    /**
     * 通过对外暴露docUuid预览文档，普通用户也可以访问；内部会做数据权限校验
     * @param docUuid 文档对外唯一uuid，不暴露数据库主键
     * @param currentUserContext 当前登录用户上下文
     * @return DownloadedDocument 文件流
     */
    @Override
    public Mono<DownloadedDocument> previewDocumentByDocUuid(String docUuid, CurrentUserContext currentUserContext) {
        return Mono.fromCallable(() -> {
                    Document doc = documentMapper.selectOne(Wrappers.<Document>lambdaQuery()
                            .eq(Document::getDocUuid, docUuid));
                    if (doc == null) {
                        throw new IllegalArgumentException("Document not found: " + docUuid);
                    }
                    // 数据权限校验：判断当前用户是否有权限访问该文档
                    if (!documentPermissionManager.canAccessDocument(currentUserContext, doc)) {
                        throw new org.springframework.security.access.AccessDeniedException("Document preview denied");
                    }
                    if (!StringUtils.hasText(doc.getObjectKey())) {
                        throw new IllegalStateException("Source file objectKey is missing");
                    }
                    return doc;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(doc -> Mono.usingWhen(
                        objectStorageService.getObject(doc.getObjectKey()),
                        is -> Mono.just(new DownloadedDocument(doc.getFileName(), is)),
                        is -> Mono.empty(),
                        (is, err) -> closeQuietly(is),
                        this::closeQuietly
                ));
    }

    /**
     * 获取用户权限范围内可以访问的全部标签集合
     * @param currentUserContext 当前用户上下文
     * @param searchScope 检索范围（spaceCodes）
     * @return 去重排序后的标签列表
     */
    @Override
    public Mono<List<String>> getAccessibleTags(CurrentUserContext currentUserContext, SearchScope searchScope) {
        return Mono.fromCallable(() -> {
                    // 构建带权限过滤的查询条件，只查询该用户可见文档
                    QueryWrapper<Document> query = documentPermissionManager.buildAccessibleDocumentQuery(currentUserContext, searchScope);
                    query.select("tags")
                            .isNotNull("tags");
                    List<Document> docs = documentMapper.selectList(query);
                    if (docs == null || docs.isEmpty()) {
                        return Collections.<String>emptyList();
                    }
                    // 提取所有标签，去重、trim、排序返回
                    return docs.stream()
                            .map(Document::getTags)
                            .filter(Objects::nonNull)
                            .flatMap(List::stream)
                            .filter(StringUtils::hasText)
                            .map(String::trim)
                            .distinct()
                            .sorted()
                            .collect(Collectors.toList());
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 获取当前用户有权访问的知识库spaceCode集合
     * 底层执行带权限过滤的SQL查询，group by space_code去重
     * @param currentUserContext 用户上下文
     * @return 用户可访问的知识库spaceCode列表
     */
    @Override
    public Mono<List<String>> getAccessibleSpaceCodes(CurrentUserContext currentUserContext) {
        return Mono.fromCallable(() -> {
                    QueryWrapper<Document> query = documentPermissionManager.buildAccessibleDocumentQuery(currentUserContext, SearchScope.empty());
                    query.select("space_code")
                            .isNotNull("space_code")
                            .groupBy("space_code")
                            .orderByAsc("space_code");
                    return documentMapper.selectList(query).stream()
                            .map(Document::getSpaceCode)
                            .filter(StringUtils::hasText)
                            .distinct()
                            .toList();
                })
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 解析并生成【实际生效的检索范围】resolveEffectiveSearchScope
     * 安全作用：前端传入的spaceCodes不可信，校验用户是否有权访问请求的知识库空间
     * 分支1：前端传spaceCodes，调用validateRequestedSpaces做合法性校验；任意space无权直接抛异常拒绝请求
     * 分支2：前端不传spaceCodes，自动取用户的defaultSpaceCode作为检索空间，再做校验
     * @param currentUserContext 当前登录用户上下文
     * @param requestedScope 前端http请求传入的SearchScope
     * @return Mono<SearchScope> 经过权限校验后真正用于向量检索的生效SearchScope
     */
    @Override
    public Mono<SearchScope> resolveEffectiveSearchScope(CurrentUserContext currentUserContext, SearchScope requestedScope) {
        // 空值保护，如果前端传入null，则使用空SearchScope
        SearchScope safeRequestedScope = requestedScope == null ? SearchScope.empty() : requestedScope;
        List<String> requestedSpaces = safeRequestedScope.requestedSpaceCodes();
        // 前端传入了知识库空间编码
        if (!requestedSpaces.isEmpty()) {
            return validateRequestedSpaces(currentUserContext, safeRequestedScope);
        }
        // 前端没有传spaceCodes，使用用户配置的默认知识库空间
        String defaultSpace = currentUserContext == null ? null : currentUserContext.defaultSpaceCode();
        if (!StringUtils.hasText(defaultSpace)) {
            return Mono.error(new IllegalStateException("请先配置默认知识空间。"));
        }
        // 组装默认范围：space为用户默认空间，tags沿用前端传入tags
        SearchScope defaultScope = new SearchScope(List.of(defaultSpace), safeRequestedScope.requestedTags());
        return validateRequestedSpaces(currentUserContext, defaultScope);
    }

    /**
     * 校验请求的spaceCode全部属于用户可访问集合；出现任意非法space直接抛出异常阻断请求，不做静默过滤
     * @param currentUserContext 用户上下文
     * @param searchScope 待校验的搜索范围
     * @return 全部合法返回searchScope；存在非法直接抛异常
     */
    private Mono<SearchScope> validateRequestedSpaces(CurrentUserContext currentUserContext, SearchScope searchScope) {
        // 查询数据库获取该用户真正有权访问的spaceCode集合
        return getAccessibleSpaceCodes(currentUserContext)
                .map(accessibleSpaces -> {
                    Set<String> accessible = new LinkedHashSet<>(accessibleSpaces);
                    // 遍历前端请求的每一个spaceCode
                    for (String requestedSpace : searchScope.requestedSpaceCodes()) {
                        if (!accessible.contains(requestedSpace)) {
                            throw new IllegalStateException("知识空间不可访问，请重新选择或配置默认知识空间。");
                        }
                    }
                    // 全部space校验通过，返回生效SearchScope
                    return searchScope;
                });
    }

    /**
     * 失败文档重试ETL：重新执行文档解析、分块、向量化流程
     * 限制：只有FAILED状态文档可以重试，最多重试3次，防止资源浪费；源文件丢失不可重试
     * @param id 文档数据库主键
     * @param userId 操作人id
     */
    @Override
    public Mono<Void> retryIngestion(String id, String userId) {
        return Mono.fromCallable(() -> documentMapper.selectById(id))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(doc -> {
                    if (doc == null) {
                        return Mono.error(new IllegalArgumentException("Document not found: " + id));
                    }
                    // 只有失败状态文档允许重试
                    if (!DocumentStatus.FAILED.name().equals(doc.getStatus())) {
                        return Mono.error(new IllegalStateException("Only FAILED documents can be retried"));
                    }
                    // 最多重试 3 次：防止反复重试无效文档浪费资源
                    if (doc.getRetryCount() != null && doc.getRetryCount() >= 3) {
                        return Mono.error(new IllegalStateException("Max retry (3) exceeded, please contact support"));
                    }
                    if (!StringUtils.hasText(doc.getObjectKey())) {
                        return markFileLostAndError(doc);
                    }
                    // 判断对象存储源文件是否还存在
                    return objectStorageService.exists(doc.getObjectKey())
                            .flatMap(exists -> {
                                if (!exists) {
                                    return markFileLostAndError(doc);
                                }
                                // 重置为 UPLOADED 使 ETL 管道从头运行：管道不支持断点续跑，必须完整重跑
                                return Mono.fromRunnable(() -> {
                                            doc.setStatus(DocumentStatus.UPLOADED.name());
                                            doc.setErrorMessage(null);
                                            doc.setErrorStack(null);
                                            doc.setAclRefreshStatus(AclRefreshStatus.PENDING.name());
                                            doc.setAclRefreshError(null);
                                            doc.setAclRefreshTime(null);
                                            doc.setUpdateDate(LocalDateTime.now());
                                            documentMapper.updateById(doc);
                                        })
                                        .subscribeOn(Schedulers.boundedElastic())
                                        .then(etlJobService.retryDocumentIngestion(doc, doc.getObjectKey(), userId));
                            });
                });
    }

    /**
     * 标记文档源文件丢失，更新状态与错误信息
     * @param doc 文档实体
     */
    private Mono<Void> markFileLostAndError(Document doc) {
        return Mono.fromRunnable(() -> {
                    doc.setStatus(DocumentStatus.FAILED.name());
                    doc.setErrorMessage("源文件已丢失，无法重试，请重新上传");
                    doc.setUpdateDate(LocalDateTime.now());
                    documentMapper.updateById(doc);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    /**
     * 更新文档ACL权限；权限变更后ACL版本号+1，加入刷新队列，同步更新Milvus向量内ACL元数据
     * @param id 文档主键id
     * @param request 权限更新请求VO
     */
    @Override
    public Mono<Void> updatePermissions(String id, DocumentPermissionUpdateRequest request) {
        return Mono.fromRunnable(() -> {
                    Document doc = documentMapper.selectById(id);
                    if (doc == null) {
                        throw new IllegalArgumentException("Document not found: " + id);
                    }
                    // 应用新的权限参数到文档实体
                    documentPermissionManager.applyPermissionUpdate(doc, request);
                    // ACL版本号自增，用于识别向量库中是否为最新权限
                    doc.setAclVersion(nextAclVersion(doc));
                    doc.setAclRefreshStatus(AclRefreshStatus.PENDING.name());
                    doc.setAclRefreshError(null);
                    doc.setAclRefreshTime(null);
                    doc.setUpdateDate(LocalDateTime.now());
                    documentMapper.updateById(doc);
                    // 加入ACL刷新任务队列
                    aclRefreshManager.enqueue(doc);
                    // 尝试立即执行一次刷新
                    boolean refreshed = aclRefreshManager.processSingle(doc.getDocUuid(), doc.getAclVersion());
                    if (!refreshed) {
                        log.warn("ACL metadata refresh queued for retry: docUuid={}, aclVersion={}",
                                doc.getDocUuid(), doc.getAclVersion());
                    }
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    /**
     * 运维接口：存量历史文档ACL元数据批量回填
     * @return 处理文档数量
     */
    @Override
    public Mono<Integer> backfillAclMetadata() {
        return Mono.fromCallable(aclRefreshManager::backfillAclMetadata)
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 静默关闭输入流，吃掉关闭时的IO异常，防止资源释放抛出异常打断响应流
     * @param is 文件输入流
     */
    private Mono<Void> closeQuietly(InputStream is) {
        return Mono.fromRunnable(() -> {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
                log.warn("Failed to close InputStream for downloaded document", e);
            }
        });
    }

    /**
     * 计算下一个ACL版本号；首次为1，后续每次权限变更+1
     * @param doc 文档实体
     * @return 新版本号
     */
    private int nextAclVersion(Document doc) {
        return doc.getAclVersion() == null ? 1 : doc.getAclVersion() + 1;
    }
}
