package net.topikachu.rag.business.document.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.auth.CurrentUserContextService;
import net.topikachu.rag.auth.SearchScope;
import net.topikachu.rag.business.document.service.DocumentService;
import net.topikachu.rag.business.document.vo.DownloadedDocument;
import net.topikachu.rag.business.document.vo.DocumentPermissionUpdateRequest;
import net.topikachu.rag.common.AjaxResult;
import net.topikachu.rag.observability.TracingSupport;
import org.jspecify.annotations.NonNull;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.security.Principal;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * RAG知识库文档控制器
 * 基于Spring WebFlux全响应式，处理文档上传、删除、下载、预览、权限、标签空间等文档相关接口
 * 两层权限控制：@PreAuthorize角色鉴权 + Service层数据权限校验
 */
@RestController
@RequestMapping("/api/v1")
@Slf4j
@RequiredArgsConstructor
public class DocumentController {

    /**
     * 文档业务层，实现上传、解析、删除、下载、权限修改核心逻辑
     */
    private final DocumentService documentService;
    /**
     * 链路追踪工具类，为当前trace span打自定义标签，用于监控、Langfuse可观测性
     */
    private final TracingSupport tracingSupport;
    /**
     * 用户上下文服务，把响应式Principal转换为系统内部用户上下文对象，用于数据权限过滤
     */
    private final CurrentUserContextService currentUserContextService;

    /**
     * 单文件上传接口
     * @param file 上传的文件，WebFlux使用FilePart接收multipart/form-data文件，Mono代表单个文件
     * @param fileName 可选，前端自定义文件名；不传使用文件原始名称
     * @param overwrite 是否覆盖已存在文档，默认false不覆盖
     * @param tags 文档标签列表，用于知识库过滤检索
     * @param locustRunId Locust压测请求标识头，用于区分压测流量，写入trace标签
     * @param principalMono WebFlux响应式获取登录用户主体，不能直接注入Principal，必须Mono包装
     * @return 统一返回Mono<AjaxResult>
     */
    @PostMapping("/docs/upload")
    @PreAuthorize("hasRole('ADMIN')") // 接口层角色鉴权：仅管理员允许上传文档
    public Mono<AjaxResult> upLoad(
            @RequestPart("file") Mono<FilePart> file,
            @RequestParam(value = "fileName", required = false) String fileName,
            @RequestParam(value = "overwrite", defaultValue = "false") boolean overwrite,
            @RequestParam(value = "tags", required = false) List<String> tags,
            @org.springframework.web.bind.annotation.RequestHeader(value = "X-Locust-Run-Id", required = false) String locustRunId,
            Mono<Principal> principalMono) {
        // 1.从Mono中获取登录用户名；未登录兜底为空字符串
        return principalMono.map(Principal::getName)
                .defaultIfEmpty("")
                .flatMap(userId -> file.flatMap(part -> documentService.upload(
                                part,
                                fileName,
                                overwrite,
                                StringUtils.hasText(userId) ? userId : null, // 用户为空传入null
                                tags))
                        // doOnSubscribe：流订阅时执行，给当前调用链路打上trace标签，记录入参信息
                        .doOnSubscribe(subscription -> tagCurrentUploadTrace(userId, fileName, overwrite, tags, locustRunId))
                        // Service处理成功，包装为全局统一响应AjaxResult
                        .map(AjaxResult::success));
    }

    /**
     * 批量文件上传接口
     * consumes显式指定接收multipart/form-data表单；produces指定输出JSON
     * @param files Flux<FilePart>，Flux代表0~N个文件，接收多文件
     * @param overwrite 是否覆盖已存在文档
     * @param tags 批量文档共用标签
     * @param principalMono 登录用户响应式对象
     * @return AjaxResult
     */
    @PostMapping(path = "/upload/batch", consumes = MediaType.MULTIPART_FORM_DATA_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    @PreAuthorize("hasRole('ADMIN')")
    public Mono<AjaxResult> uploadBatch(
            @RequestPart("files") Flux<FilePart> files,
            @RequestParam(defaultValue = "false") boolean overwrite,
            @RequestParam(value = "tags", required = false) List<String> tags,
            @NonNull Mono<Principal> principalMono) {
        return principalMono.map(Principal::getName)
                .defaultIfEmpty("")
                .flatMap(userId -> {
                    // 有效用户id：空字符串转为null传给service
                    String effectiveUserId = StringUtils.hasText(userId) ? userId : null;
                    log.info("Batch upload requested by user={}, overwrite={}, tags={}", effectiveUserId, overwrite, tags);
                    // 调用service批量上传，内部处理多文件流、存储MinIO、文档解析分块、向量入库
                    return documentService.uploadBatch(files, overwrite, effectiveUserId, tags)
                            .map(AjaxResult::success);
                });
    }

    /**
     * 分页查询文档列表接口
     * @param page 页码，从1开始
     * @param size 每页条数，默认10
     * @param keyword 可选，文档名称关键词模糊搜索
     * @return 分页文档数据包装AjaxResult
     */
    @GetMapping("/docs")
    @PreAuthorize("hasRole('ADMIN')")
    public Mono<AjaxResult> list(
            @RequestParam(defaultValue = "1") int page,
            @RequestParam(defaultValue = "10") int size,
            @RequestParam(required = false) String keyword) {
        return documentService.listDocuments(page, size, keyword)
                .map(AjaxResult::success);
    }

    /**
     * 根据数据库主键id删除单个文档
     * @param id 文档数据库主键ID
     * @return AjaxResult成功响应，无返回数据
     */
    @DeleteMapping("/docs/{id}")
    @PreAuthorize("hasRole('ADMIN')")
    public Mono<AjaxResult> remove(@PathVariable String id) {
        return documentService.removeDocumentById(id)
                .thenReturn(AjaxResult.success());
    }

    /**
     * 文档下载接口：返回二进制文件流，浏览器弹出下载框
     * @param id 文档数据库主键ID
     * @return ResponseEntity<Resource>，直接输出文件二进制流，不使用AjaxResult包装
     */
    @GetMapping("/docs/{id}/download")
    @PreAuthorize("hasRole('ADMIN')")
    public Mono<ResponseEntity<Resource>> download(@PathVariable String id) {
        return documentService.downloadDocumentById(id)
                .map(this::toDownloadResponse);
    }

    /**
     * 原文预览接口：通过对外暴露docUuid拉取文件，浏览器在线打开PDF/TXT，不弹出下载
     * 普通用户和管理员均可访问；Service内部做数据权限校验，判断该用户是否有权限访问该文档
     * @param docUuid 文档对外唯一uuid，不是数据库主键，对外接口避免暴露DB主键
     * @param principalMono 登录用户
     * @return ResponseEntity<Resource> 文件二进制流
     */
    // 原文预览：通过 docUuid 从 MinIO 拉取文件，返回二进制流供浏览器直接打开
    @GetMapping("/docs/by-uuid/{docUuid}/preview")
    @PreAuthorize("hasAnyRole('USER', 'ADMIN')")
    public Mono<ResponseEntity<Resource>> preview(@PathVariable String docUuid, Mono<Principal> principalMono) {
        return principalMono.map(Principal::getName)
                .flatMap(username -> documentService.previewDocumentByDocUuid(
                        docUuid,
                        currentUserContextService.resolveByUsername(username)))  // 校验用户是否有权访问该文档
                .map(this::toPreviewResponse);
    }

    /**
     * 批量删除文档接口
     * @param ids 请求体传入文档id集合
     * @return AjaxResult成功
     */
    @DeleteMapping("/docs")
    @PreAuthorize("hasRole('ADMIN')")
    public Mono<AjaxResult> removeBatch(@RequestBody List<String> ids) {
        return documentService.removeDocumentsBatch(ids)
                .thenReturn(AjaxResult.success());
    }

    /**
     * 获取当前用户有权访问的全部标签集合
     * @param spaceCodes 可选，知识库空间编码集合，按空间过滤标签
     * @param principalMono 登录用户
     * @return 用户权限范围内可见标签
     */
    @GetMapping("/tags")
    @PreAuthorize("hasAnyRole('USER', 'ADMIN')")
    public Mono<AjaxResult> getTags(@RequestParam(value = "spaceCodes", required = false) List<String> spaceCodes,
                                    Mono<Principal> principalMono) {
        return principalMono.map(Principal::getName)
                .flatMap(username -> documentService.getAccessibleTags(
                        currentUserContextService.resolveByUsername(username),
                        new SearchScope(spaceCodes, List.of()))) // SearchScope：定义查询数据权限范围
                .map(AjaxResult::success);
    }

    /**
     * 获取当前登录用户可以访问的知识库空间编码列表
     * @param principalMono 登录用户
     * @return 用户可访问spaceCode集合
     */
    @GetMapping("/spaces")
    @PreAuthorize("hasAnyRole('USER', 'ADMIN')")
    public Mono<AjaxResult> getSpaces(Mono<Principal> principalMono) {
        return principalMono.map(Principal::getName)
                .flatMap(username -> documentService.getAccessibleSpaceCodes(
                        currentUserContextService.resolveByUsername(username)))
                .map(AjaxResult::success);
    }

    /**
     * 文档重新解析重试接口
     * 当文档解析、分块、向量化失败后，调用此接口重新执行ingestion流程
     * @param id 文档数据库主键
     * @param principalMono 操作人登录用户
     * @return AjaxResult，提示重试任务已启动
     */
    @PostMapping("/docs/{id}/retry")
    @PreAuthorize("hasRole('ADMIN')")
    public Mono<AjaxResult> retry(@PathVariable String id, Mono<Principal> principalMono) {
        return principalMono.map(Principal::getName)
                .defaultIfEmpty("")
                .flatMap(userId -> {
                    String effectiveUserId = StringUtils.hasText(userId) ? userId : null;
                    log.info("Retry ingestion requested by user={}, docId={}", effectiveUserId, id);
                    return documentService.retryIngestion(id, effectiveUserId)
                            .thenReturn(AjaxResult.success("Retry started", null));
                });
    }

    /**
     * PATCH局部更新文档ACL权限接口
     * @param id 文档主键ID
     * @param request 请求体：封装待更新的权限参数
     * @return AjaxResult成功
     */
    @PatchMapping("/docs/{id}/permissions")
    @PreAuthorize("hasRole('ADMIN')")
    public Mono<AjaxResult> updatePermissions(@PathVariable String id,
                                              @RequestBody DocumentPermissionUpdateRequest request) {
        return documentService.updatePermissions(id, request)
                .thenReturn(AjaxResult.success());
    }

    /**
     * 运维接口：历史存量文档ACL元数据回填
     * 早期创建文档缺少权限元字段，执行该接口批量补全元数据
     * @return 返回处理数量count
     */
    @PostMapping("/docs/backfill-acl-metadata")
    @PreAuthorize("hasRole('ADMIN')")
    public Mono<AjaxResult> backfillAclMetadata() {
        return documentService.backfillAclMetadata()
                .map(count -> AjaxResult.success("ACL metadata backfill submitted", count));
    }

    /**
     * 为当前链路Trace添加自定义标签，用于监控排查问题，对接Langfuse
     * @param userId 操作人用户id
     * @param fileName 文件名称
     * @param overwrite 是否覆盖
     * @param tags 文档标签集合
     * @param locustRunId 压测任务ID
     */
    private void tagCurrentUploadTrace(String userId,
                                       String fileName,
                                       boolean overwrite,
                                       List<String> tags,
                                       String locustRunId) {
        Map<String, Object> traceTags = new LinkedHashMap<>();
        traceTags.put("langfuse.user.id", userId);
        traceTags.put("document.file_name", fileName);
        traceTags.put("document.overwrite", overwrite);
        // null标签转为空字符串，避免trace上报null值
        traceTags.put("document.tags", tags == null ? "" : String.join(",", tags));
        traceTags.put("locust.run_id", locustRunId);
        tracingSupport.tagCurrent(traceTags);
    }

    /**
     * 组装下载响应：attachment模式，浏览器弹出下载保存窗口
     * @param document DownloadedDocument VO，包含文件名与文件输入流
     * @return ResponseEntity<Resource> http响应
     */
    private ResponseEntity<Resource> toDownloadResponse(DownloadedDocument document) {
        // attachment：附件下载模式；设置UTF‑8防止中文文件名乱码
        ContentDisposition contentDisposition = ContentDisposition.attachment()
                .filename(document.fileName(), StandardCharsets.UTF_8)
                .build();
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM) // 通用二进制流
                .header(HttpHeaders.CONTENT_DISPOSITION, contentDisposition.toString())
                .body(new InputStreamResource(document.inputStream())); // InputStreamResource包装输入流为Spring Resource
    }

    /**
     * 组装预览响应：inline模式，浏览器直接在线打开文件，不触发下载弹窗
     * @param document DownloadedDocument VO
     * @return ResponseEntity<Resource>
     */
    private ResponseEntity<Resource> toPreviewResponse(DownloadedDocument document) {
        // inline：在线预览模式
        ContentDisposition contentDisposition = ContentDisposition.inline()
                .filename(document.fileName(), StandardCharsets.UTF_8)
                .build();
        return ResponseEntity.ok()
                .contentType(detectMediaType(document.fileName())) // 根据后缀自动设置MIME类型
                .header(HttpHeaders.CONTENT_DISPOSITION, contentDisposition.toString())
                .body(new InputStreamResource(document.inputStream()));
    }

    /**
     * 根据文件名后缀探测MIME媒体类型，用于浏览器正确渲染预览
     * @param fileName 文件名称
     * @return MediaType
     */
    private MediaType detectMediaType(String fileName) {
        if (fileName == null) {
            return MediaType.APPLICATION_OCTET_STREAM;
        }
        // 转小写，ROOT语言环境避免本地化特殊字符问题
        String normalized = fileName.toLowerCase(java.util.Locale.ROOT);
        if (normalized.endsWith(".pdf")) {
            return MediaType.APPLICATION_PDF;
        }
        if (normalized.endsWith(".txt") || normalized.endsWith(".md")) {
            return MediaType.TEXT_PLAIN;
        }
        // 未知后缀统一返回二进制流
        return MediaType.APPLICATION_OCTET_STREAM;
    }
}
