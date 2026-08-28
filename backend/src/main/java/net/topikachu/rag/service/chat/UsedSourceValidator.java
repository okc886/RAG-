package net.topikachu.rag.service.chat;

import lombok.extern.slf4j.Slf4j;
import org.springframework.ai.document.Document;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class UsedSourceValidator {

    // 对外给用户展示的提示文案：来源不可靠
    public static final String UNRELIABLE_SOURCE_MESSAGE = "根据当前知识库，暂未找到合理解答。";
    // 下面全部是错误原因编码，日志、异常对象里面记录，方便排查
    public static final String REASON_ANSWER_MISSING = "answer_missing";                     // 回答为空
    public static final String REASON_USED_SOURCES_EMPTY = "used_sources_empty";             // factual回答但是没有写任何引用
    public static final String REASON_EVIDENCE_ID_MISSING = "evidence_id_missing";           // 引用ID是空字符串
    public static final String REASON_EVIDENCE_ID_NOT_IN_CANDIDATES = "evidence_id_not_in_candidates"; // 模型编造ID，不在候选池
    public static final String REASON_INVALID_ANSWER_TYPE = "invalid_answer_type";           // answerType不是factual/refusal
    public static final String REASON_REFUSAL_SOURCES_NOT_EMPTY = "refusal_sources_not_empty"; // 拒绝回答，却带上引用

    /**
     * 对外核心校验方法
     * @param result LLM结构化输出：answer、answerType、usedSources(模型声称引用的evidence_id列表)
     * @param candidates 向量检索拿到的真实候选文档池
     * @return 校验通过之后，组装好的UsedSource列表，给前端展示参考文献
     * 校验失败：抛出SourceValidationException，上层GroundedTurnModule捕获，不执行commit持久化
     */
    public List<UsedSource> validate(SourcedAnswerResult result, List<Document> candidates) {
        // ----------规则1：回答文本不能为空----------
        if (result == null || !StringUtils.hasText(result.answer())) {
            throw validationFailure(REASON_ANSWER_MISSING, null, candidates);
        }

        // ----------规则2：answerType只能二选一 factual / refusal----------
        boolean refusal = "refusal".equalsIgnoreCase(result.answerType());
        boolean factual = "factual".equalsIgnoreCase(result.answerType());
        if (!refusal && !factual) {
            throw validationFailure(REASON_INVALID_ANSWER_TYPE, result, candidates);
        }

        // 获取大模型输出的引用ID列表
        List<String> requestedSources = result.usedSources() == null ? List.of() : result.usedSources();

        // ----------规则3：refusal拒绝回答场景：模型说知识库没有答案，就绝对不能带引用ID----------
        // 如果模型说“我找不到答案”，但是又写了引用ID，代表模型输出混乱，校验失败
        if (refusal) {
            if (!requestedSources.isEmpty()) {
                throw validationFailure(REASON_REFUSAL_SOURCES_NOT_EMPTY, result, candidates);
            }
            // 拒绝回答，返回空的引用列表
            return List.of();
        }

        // ----------规则4：factual事实回答场景：必须至少写1条引用证据ID----------
        if (requestedSources.isEmpty()) {
            throw validationFailure(REASON_USED_SOURCES_EMPTY, result, candidates);
        }

        // ----------规则5：把候选文档池构建Map，key=evidence_id，快速查找 O(1)----------
        // candidates是检索返回Document列表，循环把evidence_id作为key存入map
        Map<String, Document> candidatesByEvidenceId = new LinkedHashMap<>();
        for (Document candidate : candidates == null ? List.<Document>of() : candidates) {
            String evidenceId = evidenceId(candidate);
            if (StringUtils.hasText(evidenceId)) {
                candidatesByEvidenceId.put(evidenceId, candidate);
            }
        }

        // ----------规则6：逐条校验模型输出的每一个evidence_id----------
        List<UsedSource> validated = new ArrayList<>();
        for (String requestedEvidenceId : requestedSources) {
            // 引用ID不能是空
            if (!StringUtils.hasText(requestedEvidenceId)) {
                throw validationFailure(REASON_EVIDENCE_ID_MISSING, result, candidates);
            }
            // 去map里面查找模型给的ID；找不到=幻觉编造证据，抛异常
            Document candidate = candidatesByEvidenceId.get(requestedEvidenceId.trim());
            if (candidate == null) {
                throw validationFailure(REASON_EVIDENCE_ID_NOT_IN_CANDIDATES, result, candidates);
            }
            // ID合法，把Document转为UsedSource对象
            validated.add(fromDocument(candidate));
        }

        // ----------规则7：引用去重：同一个文档同一个页码，多条引用只保留1条，前端展示不重复----------
        return collapseDisplayedSources(validated);
    }

    /**
     * Document → UsedSource
     * UsedSource：前端展示参考文献的实体，需要evidenceId、文件名、页码、文件类型等元数据
     */
    private UsedSource fromDocument(Document document) {
        Map<String, Object> metadata = document.getMetadata();
        return new UsedSource(
                evidenceId(document),
                stringValue(metadata.get("doc_uuid")),
                stringValue(metadata.get("file_name")),
                sourceLocation(metadata),
                fileType(stringValue(metadata.get("file_name"))));
    }

    /**
     * 引用去重
     * key = docUuid | pageNumber
     * 同一篇文档同一个页面，模型多次引用，前端只显示一次，避免列表一堆重复
     */
    private List<UsedSource> collapseDisplayedSources(List<UsedSource> sources) {
        Map<String, UsedSource> unique = new LinkedHashMap<>();
        for (UsedSource source : sources) {
            if (source == null || !StringUtils.hasText(source.docUuid())) {
                continue;
            }
            // 文档uuid + 页码作为唯一key
            String key = source.docUuid() + "|" + (source.pageNumber() == null ? "" : source.pageNumber());
            unique.putIfAbsent(key, source);
        }
        return List.copyOf(unique.values());
    }

    /**
     * 获取文档的evidence_id
     * 优先取metadata["evidence_id"]；没有就使用Document本身的id
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

    // 对象安全转字符串，null保护
    private String stringValue(Object value) {
        return value == null ? null : value.toString();
    }

    /**
     * 解析展示用的位置信息，给前端参考文献显示页码/面包屑
     * 优先级：
     * 1.source_location 面包屑（docx/md文档，比如：规章制度>员工考勤）
     * 2.page_start / page_end PDF页码范围
     * 3.parent_index 兜底，显示“片段0、片段1”
     */
    private Object sourceLocation(Map<String, Object> metadata) {
        // ①优先面包屑路径
        Object sourceLocation = metadata.get("source_location");
        if (sourceLocation != null && StringUtils.hasText(sourceLocation.toString())) {
            return sourceLocation.toString().trim();
        }
        // ②PDF页码范围
        Object pageStart = metadata.get("page_start");
        Object pageEnd = metadata.get("page_end");
        if (pageStart != null && pageEnd != null) {
            String start = pageStart.toString();
            String end = pageEnd.toString();
            // 同一页就输出单数字，跨页输出 "2‑3"
            return start.equals(end) ? pageStart : start + "-" + end;
        }
        Object pageNumber = metadata.get("page_number");
        if (pageNumber != null) {
            return pageNumber;
        }
        // ③兜底片段索引
        Object parentIndex = metadata.get("parent_index");
        if (parentIndex != null) {
            return "片段" + parentIndex;
        }
        return null;
    }

    /**
     * 根据文件名截取后缀，pdf / docx / md
     */
    private String fileType(String fileName) {
        if (!StringUtils.hasText(fileName)) {
            return null;
        }
        int idx = fileName.lastIndexOf('.');
        if (idx < 0 || idx == fileName.length() - 1) {
            return null;
        }
        return fileName.substring(idx + 1).toLowerCase(java.util.Locale.ROOT);
    }

    /**
     * 构造校验失败异常：打印警告日志，返回SourceValidationException
     * reason：错误编码，方便日志排查；异常message是给用户看的友好提示
     */
    private SourceValidationException validationFailure(String reason, SourcedAnswerResult result, List<Document> candidates) {
        log.warn("Used source validation failed: reason={}, answerType={}, requestedSources={}, candidateCount={}",
                reason,
                result == null ? null : result.answerType(),
                result == null || result.usedSources() == null ? 0 : result.usedSources().size(),
                candidates == null ? 0 : candidates.size());
        return new SourceValidationException(UNRELIABLE_SOURCE_MESSAGE, reason);
    }
}
