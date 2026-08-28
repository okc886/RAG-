package net.topikachu.rag.service.etl;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.business.document.entity.DocumentStatus;
import net.topikachu.rag.business.document.entity.KnowledgeParentBlock;
import net.topikachu.rag.business.document.mapper.DocumentMapper;
import net.topikachu.rag.observability.TracingSupport;
import net.topikachu.rag.service.etl.fileParseStrategy.FileParseStrategy;
import net.topikachu.rag.service.etl.fileParseStrategy.FileParseStrategyFactory;
import org.springframework.ai.document.Document;
import org.springframework.ai.reader.tika.TikaDocumentReader;
import org.springframework.ai.transformer.splitter.TextSplitter;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.util.*;

/**
 * ETL 摄取管线：驱动文档状态机 UPLOADED → SPLITTING → VECTORIZING → COMPLETED/FAILED
 * 协调读取、分块、向量化写入及失败补偿清理。
 * 核心职责：文档解析、父子块切分、MySQL+Milvus异构存储写入、异常回滚补偿、链路埋点追踪
 */
@Component
@Slf4j
public class EtlPipeline {

    // 文件读取工具类
    private final DocReader documentReader;
    // 向量写入器，负责把子块写入Milvus向量库
    private final HybridVectorWriter hybridVectorWriter;
    // 文本切分器：把1200字父块切为200字子块
    private final TextSplitter textSplitter;
    // document文档主表Mapper
    private final DocumentMapper documentMapper;
    // 构建子块metadata元数据（ACL权限、标签、来源信息）
    private final DocumentChunkMetadataBuilder metadataBuilder;
    // 链路追踪工具，生成trace/span，用于排查ETL慢查询、报错
    private final TracingSupport tracingSupport;
    // 文件解析策略工厂，根据后缀选择PDF/Word等专用解析器
    private final FileParseStrategyFactory fileParseStrategyFactory;
    // 文档状态管理器，修改数据库中document的状态字段
    private final EtlStatusManager etlStatusManager;
    // 父块数据库服务，操作knowledge_parent_block表
    private final KnowledgeParentBlockService parentBlockService;

    /**
     * readAndSplit阶段超时60秒
     * 解析、拆分文本受CPU、Tika解析速度影响，超过时间直接判定ETL失败
     */
    @org.springframework.beans.factory.annotation.Value("${rag.etl.timeout.read-split-seconds:60}")
    private long SplitTimeoutSeconds;

    /**
     * 向量化入库阶段超时300秒(5分钟)
     * 需要调用Embedding接口、网络读写Milvus，IO耗时大，给更长超时时间
     */
    @org.springframework.beans.factory.annotation.Value("${rag.etl.timeout.vectorize-seconds:300}")
    private long vectorizeTimeoutSeconds;

    /**
     * 纯读取文件超时900秒，预留给扫描版PDF OCR识别，OCR非常耗时
     * 当前代码没有直接使用该变量，配置预留
     */
    @org.springframework.beans.factory.annotation.Value("${rag.etl.timeout.read-seconds:900}")
    private long readingTimeoutSeconds;

    // 父块文本大小：1200字符，存MySQL，检索后拿给LLM完整上下文
    @org.springframework.beans.factory.annotation.Value("${rag.retrieval.parent-block-size:1200}")
    private int parentBlockSize;

    // 父块之间重叠字符数200，避免语义被切割断裂
    @org.springframework.beans.factory.annotation.Value("${rag.retrieval.parent-block-overlap:200}")
    private int parentBlockOverlap;

    // 构造函数注入全部依赖，Spring容器自动装配
    public EtlPipeline(HybridVectorWriter hybridVectorWriter,
                       TextSplitter textSplitter,
                       DocReader documentReader,
                       DocumentMapper documentMapper,
                       DocumentChunkMetadataBuilder metadataBuilder,
                       TracingSupport tracingSupport,
                       FileParseStrategyFactory fileParseStrategyFactory,
                       EtlStatusManager etlStatusManager,
                       KnowledgeParentBlockService parentBlockService) {
        this.hybridVectorWriter = hybridVectorWriter;
        this.textSplitter = textSplitter;
        this.documentReader = documentReader;
        this.documentMapper = documentMapper;
        this.metadataBuilder = metadataBuilder;
        this.tracingSupport = tracingSupport;
        this.fileParseStrategyFactory = fileParseStrategyFactory;
        this.etlStatusManager = etlStatusManager;
        this.parentBlockService = parentBlockService;
    }

    /**
     * 目录扫描批量摄入【预留接口，未实现业务逻辑】
     * 功能设想：扫描某个文件夹，批量导入文件夹内全部文件
     * 逻辑：计算文件SHA256哈希，如果数据库已经存在该哈希，说明文件已经导入，跳过处理
     * 当前仅框架代码，实际摄入逻辑未编写，返回空流
     */
    public Flux<Document> ingestionFlux() {
        return documentReader.scanDirectory()
                .flatMap(path -> Mono.fromCallable(() -> {
                            // 计算文件SHA256哈希，用于文件去重
                            String hash = computeHash(path);
                            // 查询数据库，判断这个哈希的文件是否已经导入过
                            Long count = documentMapper.selectCount(
                                    Wrappers.<net.topikachu.rag.business.document.entity.Document>lambdaQuery()
                                            .eq(net.topikachu.rag.business.document.entity.Document::getFileHash, hash));
                            if (count > 0)
                                return null;
                            return hash;
                        }).subscribeOn(Schedulers.boundedElastic()) // 文件IO阻塞操作，丢到boundedElastic线程池，不阻塞webflux主线程
                        .flatMapMany(hash -> {
                            if (hash == null)
                                return Mono.empty();
                            return Mono.empty(); // 真正的批量摄入逻辑未实现
                        }));
    }

    /**
     * ETL主入口方法：处理单个文档
     * @param path 文件磁盘路径
     * @param docUuid 文档全局唯一ID
     * @param userId 操作人用户ID
     * @param tags 文档业务标签
     * @return Mono<Void> 响应式异步结果
     */
    public Mono<Void> ingestionByPath(Path path, String docUuid, String userId, List<String> tags) {
        // 封装全流程上下文对象，把所有公共参数打包，不用每个方法传一堆入参
        EtlContext ctx = EtlContext.of(path, docUuid, userId, tags);

        // 组装完整ETL流水线链式调用
        // 每个stage设置独立超时，不是全局总超时，哪个阶段超时哪个阶段失败
        Mono<Void> pipeline = readAndSplit(ctx)
                .timeout(java.time.Duration.ofSeconds(SplitTimeoutSeconds)) // 第一阶段超时：解析拆分超时
                .flatMap(parentChildDocs -> writeParentBlocksAndVectors(ctx, parentChildDocs)) // 拿到内存中分好的父子块，执行入库
                .timeout(java.time.Duration.ofSeconds(vectorizeTimeoutSeconds)) // 第二阶段超时：向量化入库超时
                .then(completeStatus(ctx)) // 全部成功：更新文档状态为COMPLETED
                .doOnSuccess(v -> log.info("IngestionByPath finished: {}", ctx.path())) // 成功打印日志
                .onErrorResume(error -> failAndCleanup(ctx, error)); // 任意步骤异常，执行失败补偿清理逻辑

        // 包装链路追踪，整个ETL作为一条trace，方便监控排查问题
        return tracingSupport.traceMono("etl.ingestion", ctx.traceTags(), pipeline);
    }

    /**
     * EtlContext：ETL全流程上下文记录，record不可变数据载体
     * 保存文件路径、文档ID、用户ID、文件名后缀、链路标签，全链路透传
     */
    public record EtlContext(
            Path path,
            String docUuid,
            String userId,
            List<String> tags,
            String fileName,
            String fileExtension,
            Map<String, Object> traceTags
    ) {
        /**
         * 构造上下文对象，自动解析文件名、后缀，组装链路追踪标签
         */
        public static EtlContext of(Path path, String docUuid, String userId, List<String> tags) {
            String fileName = path.getFileName().toString();
            String fileExt = extension(fileName); // 获取文件后缀，小写
            Map<String, Object> traceTags = etlTraceTags(path, docUuid, userId, tags);
            traceTags.put("document.file_ext", fileExt);
            return new EtlContext(
                    path,
                    docUuid,
                    userId,
                    tags,
                    fileName,
                    fileExt,
                    traceTags
            );
        }

        /**
         * 在原有traceTags基础上增加单个k-v，返回新map，不修改原对象
         */
        Map<String, Object> tagsWith(String key, Object value) {
            Map<String, Object> merged = new LinkedHashMap<>(traceTags);
            merged.put(key, value);
            return merged;
        }

        /**
         * 在原有traceTags基础上批量增加标签，返回新map
         */
        Map<String, Object> tagsWith(Map<String, Object> extra) {
            Map<String, Object> merged = new LinkedHashMap<>(traceTags);
            merged.putAll(extra);
            return merged;
        }
    }

    /**
     * 阶段一：readAndSplit 读取文档+文本拆分
     * 状态切换为 SPLITTING
     * 返回内存中的父子块封装对象（此时尚未落库MySQL/Milvus）
     */
    private Mono<ChunkUtils.ParentChildDocuments> readAndSplit(EtlContext ctx) {
        return tracingSupport.traceMono("etl.read_split",
                ctx.tagsWith(Map.of("etl.stage", "read_split")),
                // 1. 修改数据库文档状态为SPLITTING：正在读取拆分
                etlStatusManager.transitionTo(ctx.docUuid(), ctx.userId, DocumentStatus.SPLITTING, "Reading and splitting...")
                        .then(Mono.fromCallable(() -> {
                            // 2. 根据文件后缀，获取对应解析策略
                            FileParseStrategy strategy = fileParseStrategyFactory
                                    .getFileParseStrategyOrNull(ctx.fileExtension());
                            if (strategy != null) {
                                // 有专用解析器（pdf、docx等），使用定制解析逻辑
                                return strategy.readAndSplit(ctx.fileExtension(), ctx);
                            }
                            // 没有专用解析策略，走Tika通用兜底解析(txt、rtf等)
                            return genericReadAndSplit(ctx);
                        }).subscribeOn(Schedulers.boundedElastic())) // Tika解析是阻塞CPU操作，放到弹性线程池，禁止阻塞Reactor事件循环
                        .flatMap(parentChildDocs -> loadDocumentByDocUuid(ctx.docUuid())
                                // 查询数据库里这份文档记录，填充ACL、标签等业务元数据到父子块
                                .map(storedDoc -> enrichMetadata(ctx, parentChildDocs, storedDoc)))
                        // 打印日志：输出这份文档生成多少父块、多少子块
                        .doOnNext(parentChildDocs -> log.info(
                                "Read and split: docUuid={}, parentBlocks={}, childChunks={}",
                                ctx.docUuid(),
                                parentChildDocs.parentBlocks().size(),
                                parentChildDocs.childDocuments().size())));
    }

    /**
     * Tika通用兜底解析：没有专用解析策略时进入这里，如txt文件
     * @return 内存中生成好的父子块对象
     */
    private ChunkUtils.ParentChildDocuments genericReadAndSplit(EtlContext ctx) {
        // TikaDocumentReader：Apache Tika万能解析器，提取文件文本
        List<Document> docs = new TikaDocumentReader(ctx.path().toUri().toString()).get();
        // 文本清洗：过滤乱码、无效字符，过滤空片段
        docs = sanitizeDocuments(ctx.path(), docs);
        // 清洗完之后没有可用文本，直接抛出异常，ETL失败
        if (docs.isEmpty()) {
            throw new IllegalStateException("No usable text extracted: " + ctx.path());
        }
        // 执行固定窗口切分，构建父块、子块
        return buildFixedWindowParentChildDocuments(ctx, docs);
    }

    /**
     * 构建父子块核心逻辑，全部内存操作，不操作数据库
     * 1.全部文本拼接，按parentBlockSize切1200字父块
     * 2.每个父块，再切分成200字子块；子块记录parent_block_id关联父块
     */
    private ChunkUtils.ParentChildDocuments buildFixedWindowParentChildDocuments(EtlContext ctx, List<Document> docs) {
        // 拿解析出来的原始元数据
        Map<String, Object> baseMetadata = new LinkedHashMap<>(docs.get(0).getMetadata());
        // 把全部文档片段拼接成一整个大字符串
        String joinedText = docs.stream()
                .map(Document::getText)
                .filter(Objects::nonNull)
                .map(String::trim)
                .filter(text -> !text.isBlank())
                .reduce((left, right) -> left + "\n\n" + right)
                .orElse("");

        // 固定窗口切分大文本，得到多个父块文本片段（1200字，重叠200）
        List<String> pieces = ChunkUtils.splitByFixedWindow(joinedText, parentBlockSize, parentBlockOverlap);
        List<KnowledgeParentBlock> parentBlocks = new ArrayList<>(); // 父块集合，准备写入MySQL
        List<Document> childDocuments = new ArrayList<>(); // 子块集合，准备写入Milvus
        int childIndex = 1; // 子块序号，全局自增

        // 循环每一个父块文本片段
        for (int i = 0; i < pieces.size(); i++) {
            int parentIndex = i + 1;
            Map<String, Object> metadata = new LinkedHashMap<>(baseMetadata);
            metadata.put("parent_index", parentIndex);
            metadata.put("chunk_schema_version", KnowledgeParentBlockService.CHUNK_SCHEMA_VERSION);
            // 生成parent_block_id：父块唯一标识，子块靠这个ID关联父块
            metadata.put("parent_block_id", ChunkUtils.buildParentBlockId(
                    ctx.docUuid(), pieces.get(i), parentIndex));
            Document parentDocument = new Document(pieces.get(i), metadata);
            // 转为数据库实体对象，加入父块列表
            parentBlocks.add(ChunkUtils.toKnowledgeParentBlock(parentDocument));

            // 将父块再切分成多个200字子块
            List<Document> children = textSplitter.apply(List.of(parentDocument));
            for (Document child : children) {
                Map<String, Object> childMeta = child.getMetadata();
                // 子块元数据写入关联信息，检索Milvus拿到子块，通过parent_block_id回查MySQL父块
                childMeta.put("chunk_schema_version", KnowledgeParentBlockService.CHUNK_SCHEMA_VERSION);
                childMeta.put("parent_block_id", metadata.get("parent_block_id"));
                childMeta.put("parent_index", parentIndex);
                childMeta.put("child_index", childIndex);
                // evidence_id：子块全局唯一ID，用于溯源、引用来源片段
                childMeta.put("evidence_id", ChunkUtils.buildEvidenceId(ctx.docUuid(), child, childIndex));
                childMeta.put("source_location", "片段" + parentIndex);
                childDocuments.add(child);
                childIndex++;
            }
        }
        // 返回封装对象：内存中的父块列表、子块列表
        return new ChunkUtils.ParentChildDocuments(parentBlocks, childDocuments);
    }

    /**
     * enrichMetadata：元数据增强
     * 把数据库中doc的业务信息（docUuid、文件名、space空间、标签、ACL权限版本）填充到父块和子块
     * @param etlContext ETL上下文
     * @param docs 内存中生成好的父子块
     * @param storedDocument 数据库查询出来的文档记录
     * @return 填充完元数据的父子块对象
     */
    private ChunkUtils.ParentChildDocuments enrichMetadata(EtlContext etlContext,
                                                           ChunkUtils.ParentChildDocuments docs,
                                                           net.topikachu.rag.business.document.entity.Document storedDocument) {
        // 优先使用数据库保存的tags，其次使用传入上下文tags
        List<String> effectiveTags = etlContext.tags;
        if (storedDocument != null && storedDocument.getTags() != null) {
            effectiveTags = storedDocument.getTags();
        }
        // 给父块实体填充数据库字段
        for (KnowledgeParentBlock parentBlock : docs.parentBlocks()) {
            parentBlock.setDocUuid(etlContext.docUuid());
            parentBlock.setFileName(etlContext.fileName());
            parentBlock.setSpaceCode(resolveSpaceCode(storedDocument));
            parentBlock.setTags(effectiveTags == null ? List.of() : List.copyOf(effectiveTags));
            parentBlock.setAclVersion(storedDocument == null ? 1 : storedDocument.getAclVersion());
            parentBlock.setChunkSchemaVersion(KnowledgeParentBlockService.CHUNK_SCHEMA_VERSION);
        }
        // 给子块Document构建完整元数据（ACL权限过滤的核心）
        for (Document child : docs.childDocuments()) {
            Map<String, Object> rebuiltMetadata = metadataBuilder.build(
                    storedDocument,
                    etlContext.docUuid,
                    etlContext.fileName(),
                    effectiveTags,
                    child.getMetadata(),
                    storedDocument == null ? 1 : storedDocument.getAclVersion());
            child.getMetadata().clear();
            child.getMetadata().putAll(rebuiltMetadata);
        }
        return docs;
    }

    /**
     * 获取文档所属空间编码，如果为空，默认返回public公共空间
     * 防止向量没有spaceCode，检索查不到数据
     */
    private String resolveSpaceCode(net.topikachu.rag.business.document.entity.Document storedDocument) {
        if (storedDocument == null || storedDocument.getSpaceCode() == null || storedDocument.getSpaceCode().isBlank()) {
            return "public";
        }
        return storedDocument.getSpaceCode().trim();
    }

    /**
     * 阶段二 writeParentBlocksAndVectors：写入MySQL父块 + Milvus子向量
     * 状态切换VECTORIZING
     * ⚠️顺序强制约束：先写MySQL父块，再写Milvus子向量；子块依赖parent_block_id引用父块，顺序不能颠倒
     */
    private Mono<Void> writeParentBlocksAndVectors(EtlContext ctx, ChunkUtils.ParentChildDocuments parentChildDocs) {
        Map<String, Object> traceTags = ctx.tagsWith(Map.of(
                "etl.stage", "vectorize",
                "etl.parent_block_count", parentChildDocs.parentBlocks().size(),
                "etl.chunk_count", parentChildDocs.childDocuments().size()
        ));
        return tracingSupport.traceMono("etl.vectorize", traceTags,
                // 修改文档状态为VECTORIZING：正在向量化写入存储
                etlStatusManager.transitionTo(ctx.docUuid, ctx.userId, DocumentStatus.VECTORIZING, "Writing to Vector Store...")
                        // replaceForDocument：先删除该docUuid旧的全部父块，再插入本次新父块；支持文档重新上传覆盖旧数据
                        .then(parentBlockService.replaceForDocument(ctx.docUuid(), parentChildDocs.parentBlocks()))
                        .then(Mono.defer(() -> {
                            // 边界防护：全图片PDF解析不出文本，子块为空集合，跳过Milvus写入，避免Embedding接口传入空文本报错
                            if (!parentChildDocs.childDocuments().isEmpty()) {
                                return hybridVectorWriter.write(parentChildDocs.childDocuments());
                            }
                            return Mono.empty();
                        })));
    }

    /**
     * sanitizeDocuments 文本清洗
     * Tika解析出来的原始文本经常带有乱码、不可见控制字符；过滤低质量、空文本片段
     */
    private List<Document> sanitizeDocuments(Path path, List<Document> docs) {
        List<Document> sanitizedDocs = new ArrayList<>();
        for (Document doc : docs) {
            // 调用文本清洗工具，移除无效字符
            TextSanitizer.SanitizationResult result = TextSanitizer.sanitize(doc.getText());
            Object pageNumber = doc.getMetadata().get("page_number");
            String sourceLabel = pageNumber != null ? "page " + pageNumber : "document chunk";

            // 如果文本被修改过，打印日志记录移除字符数量
            if (result.wasModified()) {
                log.info("Sanitized {} from {}: removedChars={}, normalizedWhitespace={}, originalLength={}, sanitizedLength={}",
                        sourceLabel, path.getFileName(), result.removedChars(), result.normalizedWhitespace(),
                        result.originalLength(), result.text().length());
            }
            // 低质量文本告警（扫描PDF乱码多、有效文字极少）
            if (result.isLowQualityExtraction()) {
                log.warn("Low-quality {} extraction for {}: removedRatio={}, meaningfulCodePoints={}",
                        sourceLabel, path.getFileName(), result.removedRatioPercent(), result.meaningfulCodePoints());
            }
            // 清洗之后文本为空，直接丢弃这个片段，不参与后续分块
            if (result.isEffectivelyEmpty()) {
                log.warn("Dropping {} from {} after sanitization", sourceLabel, path.getFileName());
                continue;
            }
            sanitizedDocs.add(new Document(result.text(), new HashMap<>(doc.getMetadata())));
        }
        return sanitizedDocs;
    }

    /**
     * failAndCleanup 失败补偿逻辑【核心容错】
     * MySQL与Milvus无法做分布式事务，异常发生后手动清理已经写入的数据，防止孤儿脏数据
     * 流程：删除MySQL父块 → 内部联动删除Milvus向量 → 设置文档状态FAILED → 继续抛出异常给上层
     */
    private Mono<Void> failAndCleanup(EtlContext ctx, Throwable error) {
        log.error("Error during ingestionByPath: {}", ctx.path, error);

        Map<String, Object> extra = new LinkedHashMap<>();
        extra.put("etl.stage", "failed");
        extra.put("error.type", error == null ? "" : error.getClass().getSimpleName());
        extra.put("error.message", extractUserMessage(error));
        return tracingSupport.traceMono("etl.fail_cleanup", ctx.tagsWith(extra),
                // 根据docUuid删除这份文档全部父块；内部同时清理Milvus对应的子向量
                parentBlockService.deleteByDocUuid(ctx.docUuid())
                        // 更新文档状态为FAILED，保存错误信息
                        .then(etlStatusManager.transitionToFailed(ctx.docUuid(), ctx.userId(), error))
                        // 重新抛出原始异常，上层调用方可以捕获异常，不会吞掉错误
                        .then(Mono.error(error)));
    }

    /**
     * completeStatus：ETL全部成功，更新文档状态COMPLETED，文档可用于检索问答
     */
    private Mono<Void> completeStatus(EtlContext ctx) {
        return tracingSupport.traceMono("etl.finish",
                ctx.tagsWith("etl.stage", "finish"),
                etlStatusManager.transitionToCompleted(ctx.docUuid(), ctx.userId()));
    }

    /**
     * 组装链路追踪标签，把docUuid、文件名、userId等信息埋入trace，便于排查问题
     */
    private static Map<String, Object> etlTraceTags(Path path, String docUuid, String userId, List<String> tags) {
        Map<String, Object> traceTags = new LinkedHashMap<>();
        traceTags.put("document.doc_uuid", docUuid);
        traceTags.put("document.file_name", path == null ? "" : path.getFileName());
        traceTags.put("document.user_id", userId);
        traceTags.put("document.tags", tags == null ? "" : String.join(",", tags));
        return traceTags;
    }

    /**
     * 工具方法：提取文件后缀，全部小写，例如 test.pdf → pdf
     */
    private static String extension(String fileName) {
        int idx = fileName.lastIndexOf('.');
        if (idx < 0 || idx == fileName.length() - 1) {
            return "";
        }
        return fileName.substring(idx + 1).toLowerCase(Locale.ROOT);
    }

    /**
     * 响应式包装：根据docUuid查询数据库Document文档记录，阻塞查询放到boundedElastic线程池
     */
    private Mono<net.topikachu.rag.business.document.entity.Document> loadDocumentByDocUuid(String docUuid) {
        return Mono.fromCallable(() -> loadDocumentByDocUuidBlocking(docUuid))
                .subscribeOn(Schedulers.boundedElastic());
    }

    /**
     * 阻塞查询数据库文档记录
     */
    private net.topikachu.rag.business.document.entity.Document loadDocumentByDocUuidBlocking(String docUuid) {
        if (docUuid == null || docUuid.isBlank()) {
            return null;
        }
        return documentMapper.selectOne(
                Wrappers.<net.topikachu.rag.business.document.entity.Document>lambdaQuery()
                        .eq(net.topikachu.rag.business.document.entity.Document::getDocUuid, docUuid)
                        .last("LIMIT 1"));
    }

    /**
     * 提取用户可见错误信息，拿到根异常message
     * 截断500字符，防止数据库status_message字段存不下超长异常堆栈
     */
    private String extractUserMessage(Throwable e) {
        if (e == null)
            return "Unknown error";
        Throwable cause = e;
        // 循环拿到最底层根异常
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        String msg = cause.getMessage();
        if (msg == null || msg.isBlank()) {
            msg = cause.getClass().getSimpleName();
        }
        return msg.length() > 500 ? msg.substring(0, 500) : msg;
    }

    /**
     * computeHash：计算文件SHA‑256哈希，用于目录扫描模式做文件去重
     * 读取全部文件字节，生成哈希摘要，相同文件哈希值一样
     */
    private String computeHash(Path path) throws Exception {
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        try (InputStream in = Files.newInputStream(path);
             DigestInputStream dis = new DigestInputStream(in, md)) {
            byte[] buffer = new byte[8192];
            while (dis.read(buffer) != -1)
                ;
        }
        return HexFormat.of().formatHex(md.digest());
    }
}
