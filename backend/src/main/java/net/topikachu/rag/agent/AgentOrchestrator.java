package net.topikachu.rag.agent;

import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.auth.CurrentUserContext;
import net.topikachu.rag.auth.SearchScope;
import net.topikachu.rag.observability.TracingSupport;
import net.topikachu.rag.service.chat.ParentContextBlock;
import net.topikachu.rag.service.chat.ReactiveChatGateway;
import net.topikachu.rag.service.chat.strategy.ChatModelStrategy;
import org.springframework.ai.chat.client.advisor.ToolCallAdvisor;
import org.springframework.ai.chat.messages.Message;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.support.ToolCallbacks;
import org.springframework.ai.tool.ToolCallback;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.ArrayList;
import  java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Agent编排器，RAG智能代理核心调度类
 * 负责Agent多轮工具调用循环：规划 -> 知识库检索 -> 证据筛选 -> 生成回答/生成追问选项
 * 控制最大执行步数、证据数量、重复查询次数，处理证据不足追问、拒答、幻觉防护逻辑
 */
@Component
@Slf4j
public class AgentOrchestrator {

    /**
     * 证据不足时，给到前端的固定提示文案
     */
    private static final String FOLLOWUP_PROMPT = "现有证据不足，请选择一个更具体的问题继续检索。";

    private final ReactiveChatGateway reactiveChatGateway;
    private final AgentKnowledgeService knowledgeService;
    private final AgentToolBridge agentToolBridge;
    private final AgentHistorySnapshotBuilder historySnapshotBuilder;
    /**
     * Agent业务执行专用调度器，隔离Netty事件循环，执行阻塞工具逻辑
     */
    private final Scheduler agentOrchestratorScheduler;
    /**
     * 链路追踪组件，埋点记录agent调用指标
     */
    private final TracingSupport tracingSupport;
    /**
     * SpringAI工具调用增强Advisor，用于模型function call工具调用
     */
    private final ToolCallAdvisor toolCallAdvisor = ToolCallAdvisor.builder().build();

    /**
     * Agent整体执行超时时间，单位毫秒
     */
    @Value("${rag.agent.timeout-ms:12000}")
    private long timeoutMs;

    /**
     * Agent最大工具调用轮次，防止无限循环调用工具
     */
    @Value("${rag.agent.max-steps:6}")
    private int maxToolCalls;

    /**
     * 单次Agent最多收集证据片段数量上限
     */
    @Value("${rag.agent.max-evidence-count:12}")
    private int maxEvidenceCount;

    /**
     * 允许重复检索相同query最大次数，避免循环重复查询
     */
    @Value("${rag.agent.max-repeated-query-count:2}")
    private int maxRepeatedQueryCount;

    public AgentOrchestrator(ReactiveChatGateway reactiveChatGateway,
                             AgentKnowledgeService knowledgeService,
                             AgentToolBridge agentToolBridge,
                             AgentHistorySnapshotBuilder historySnapshotBuilder,
                             @Qualifier("agentOrchestratorScheduler") Scheduler agentOrchestratorScheduler,
                             TracingSupport tracingSupport) {
        this.reactiveChatGateway = reactiveChatGateway;
        this.knowledgeService = knowledgeService;
        this.agentToolBridge = agentToolBridge;
        this.historySnapshotBuilder = historySnapshotBuilder;
        this.agentOrchestratorScheduler = agentOrchestratorScheduler;
        this.tracingSupport = tracingSupport;
    }

    /**
     * Agent主入口编排方法
     * @param strategy 模型策略，区分不同大模型实例
     * @param userInput 用户原始提问
     * @param conversationId 会话id
     * @param msgId 当前消息id
     * @param currentUserContext 当前登录用户上下文
     * @param searchScope 检索范围：选定标签、选定知识空间spaceCode
     * @return Mono<AgentExecutionResult> Agent执行结果，包含证据、笔记时间线、追问选项、回答草稿、指令
     */
    public Mono<AgentExecutionResult> orchestrate(ChatModelStrategy strategy,
                                                  String userInput,
                                                  String conversationId,
                                                  String msgId,
                                                  CurrentUserContext currentUserContext,
                                                  SearchScope searchScope) {
        // 生成本次agent请求唯一id，用于日志、链路追踪
        String requestId = UUID.randomUUID().toString();
        // 获取用户预选标签
        List<String> selectedTags = searchScope == null ? List.of() : searchScope.requestedTags();
        // 获取用户预选知识空间
        List<String> selectedSpaces = searchScope == null ? List.of() : searchScope.requestedSpaceCodes();
        // Agent执行上下文：保存本轮全部可变状态：检索证据、笔记、步骤、追问候选等
        AgentExecutionContext executionContext = new AgentExecutionContext(
                requestId,
                conversationId,
                msgId,
                userInput,
                selectedTags,
                selectedSpaces);
        // 写入PLANNING规划阶段note，UI渲染时间线，提前写入避免前端时间线为空
        executionContext.addNote(AgentStage.PLANNING, "decision", "正在规划检索与回答步骤。");

        // 工具回调对象：封装知识库检索、生成追问选项等工具，在多轮tool call之间共享executionContext状态
        AgentKnowledgeTools toolObject = new AgentKnowledgeTools(
                knowledgeService,
                executionContext,
                currentUserContext,
                searchScope == null ? SearchScope.empty() : searchScope,
                agentToolBridge,
                reactiveChatGateway,
                strategy,
                Duration.ofMillis(timeoutMs),
                maxToolCalls,
                maxEvidenceCount,
                maxRepeatedQueryCount);
        // 将工具对象包装为SpringAI工具回调列表，供模型调用
        List<ToolCallback> toolCallbacks = List.of(ToolCallbacks.from(toolObject));
        // 拼接历史会话消息快照
        List<Message> messages = new ArrayList<>(historySnapshotBuilder.build(conversationId));
        // 添加当前用户提问消息
        messages.add(UserMessage.builder().text(userInput).build());

        // 构建Agent工具调用流水线：执行模型+工具循环，输出AgentResolution结构化JSON结果
        Mono<AgentExecutionResult> pipeline = reactiveChatGateway.runToolPhase(
                        strategy.getChatClient(),
                        toolPhasePrompt(), // Agent系统提示词
                        Map.of(
                                "preselectedTags", summarizeValues(selectedTags),
                                "preselectedSpaces", summarizeValues(selectedSpaces)),
                        messages,
                        List.of(toolCallAdvisor),
                        toolCallbacks,
                        toolContext(executionContext),
                        AgentResolution.class)
                // 将模型输出的结构化解析结果转换为对外返回的Agent执行结果
                .map(resolution -> toExecutionResult(executionContext, resolution))
                // 模型没有输出有效JSON，触发回落逻辑，生成追问选项
                .switchIfEmpty(Mono.fromSupplier(() -> noEvidenceFollowup(executionContext.snapshot())))
                // 捕获Agent执行异常，打印错误日志
                .doOnError(error -> log.error("Agent tool phase failed. conversationId={}, msgId={}", conversationId, msgId, error))
                // 全部agent业务逻辑运行在agent专用调度器，不阻塞netty事件循环
                .subscribeOn(agentOrchestratorScheduler);

        // 链路追踪包装，记录agent执行埋点指标
        return tracingSupport.traceMono("agent.orchestrate",
                Map.of(
                        "chat.mode", "agent",
                        "chat.conversation_id", conversationId,
                        "chat.msg_id", msgId,
                        "agent.input_chars", userInput == null ? 0 : userInput.length()),
                pipeline);
    }

    /**
     * 重载方法：简化入参，直接传入标签列表，内部封装SearchScope调用主orchestrate
     * @param strategy 模型策略
     * @param userInput 用户提问
     * @param conversationId 会话id
     * @param msgId 消息id
     * @param tags 限定检索标签
     * @return Mono<AgentExecutionResult>
     */
    public Mono<AgentExecutionResult> orchestrate(ChatModelStrategy strategy,
                                                  String userInput,
                                                  String conversationId,
                                                  String msgId,
                                                  List<String> tags) {
        return orchestrate(strategy, userInput, conversationId, msgId, null, new SearchScope(List.of(), tags));
    }

    /**
     * 将模型输出AgentResolution转换为对外输出AgentExecutionResult
     * 分支处理：followup追问、refusal拒答、normal正常回答；增加幻觉防护：未选中证据强制走追问
     * @param executionContext agent执行上下文
     * @param resolution 模型输出结构化JSON解析对象
     * @return AgentExecutionResult
     */
    private AgentExecutionResult toExecutionResult(AgentExecutionContext executionContext, AgentResolution resolution) {
        AgentExecutionSnapshot snapshot = executionContext.snapshot();
        if (resolution == null) {
            return noEvidenceFollowup(snapshot);
        }

        // 归一化模型输出type，防止LLM输出大小写混乱
        String type = normalizeType(resolution.type());
        // 归一化answerMode，normal正常回答 / refusal拒答
        String answerMode = normalizeAnswerMode(resolution.answerMode());

        // 分支1：type=followup，证据不足，返回追问按钮给前端
        if ("followup".equals(type)) {
            executionContext.addNote(AgentStage.FOLLOWUP, "decision", "当前证据不足，转为点击式追问。");
            snapshot = executionContext.snapshot();
            FollowupSuggestion suggestion = resolveFollowupSuggestion(snapshot);
            return new AgentExecutionResult(
                    List.of(),
                    List.of(),
                    sortNotes(snapshot.notes()),
                    suggestion.prompt(),
                    suggestion.options(),
                    null,
                    null,
                    "normal",
                    false);
        }

        // 分支2：answerMode=refusal，知识库无法回答，拒答模式
        if ("refusal".equals(answerMode)) {
            executionContext.addNote(AgentStage.GENERATING_FINAL, "decision", "知识库无法回答该问题，返回拒答说明。");
            snapshot = executionContext.snapshot();
            return new AgentExecutionResult(
                    List.of(),
                    List.of(),
                    sortNotes(snapshot.notes()),
                    null,
                    List.of(),
                    defaultRefusalDraft(resolution.draftAnswer()),
                    defaultRefusalInstruction(resolution.finalInstruction()),
                    "refusal",
                    false);
        }

        // 根据模型选中的evidenceId，筛选最终要使用的证据片段
        List<EvidenceSnapshot> selectedEvidence = AgentEvidenceSelector.selectEvidence(
                snapshot.retrievedEvidence(),
                resolution.selectedEvidenceIds());
        // 幻觉防护：模型要输出回答，但是没有选中任何证据，禁止回答，强制降级追问，防止大模型编造内容
        if (selectedEvidence.isEmpty()) {
            executionContext.addNote(AgentStage.FOLLOWUP, "decision", "模型未选中有效证据，回退为点击式追问。");
            return noEvidenceFollowup(executionContext.snapshot());
        }
        // 根据选中证据，筛选对应的父级上下文块
        List<ParentContextBlock> selectedParentContexts =
                AgentEvidenceSelector.selectParentContextsForEvidence(
                        snapshot.retrievedParentContexts(),
                        resolution.selectedEvidenceIds());

        // 标记进入最终生成阶段
        executionContext.addNote(AgentStage.GENERATING_FINAL, "decision", "已完成证据选择，准备生成最终答案。");
        snapshot = executionContext.snapshot();
        // 组装正常回答结果返回
        return new AgentExecutionResult(
                List.copyOf(selectedEvidence),
                selectedParentContexts,
                sortNotes(snapshot.notes()),
                null,
                List.of(),
                resolution.draftAnswer(),
                resolution.finalInstruction(),
                "normal",
                resolution.finalInstruction() != null && !resolution.finalInstruction().isBlank());
    }

    /**
     * 无证据回落：生成追问结果对象
     * @param snapshot agent执行快照
     * @return AgentExecutionResult
     */
    private AgentExecutionResult noEvidenceFollowup(AgentExecutionSnapshot snapshot) {
        FollowupSuggestion suggestion = resolveFollowupSuggestion(snapshot);
        return new AgentExecutionResult(
                List.of(),
                List.of(),
                sortNotes(snapshot.notes()),
                suggestion.prompt(),
                suggestion.options(),
                null,
                null,
                "normal",
                false);
    }

    /**
     * 解析追问候选：优先使用工具generateFollowupOptions产出的候选；不满足条件则走兜底生成逻辑
     * @param snapshot agent快照
     * @return FollowupSuggestion 追问提示+选项
     */
    private FollowupSuggestion resolveFollowupSuggestion(AgentExecutionSnapshot snapshot) {
        FollowupOptionsResult candidate = snapshot.followupOptionsCandidate();
        // 校验工具输出的追问候选是否合法：status=ok，恰好2个选项，UI只渲染两个按钮
        if (candidate != null && "ok".equals(candidate.status()) && isValidFollowupCandidate(candidate)) {
            return new FollowupSuggestion(FOLLOWUP_PROMPT, List.copyOf(candidate.options()), "tool");
        }
        // 工具输出不合法，进入兜底逻辑，后端自己生成追问选项
        return buildFallbackSuggestion(snapshot.originalUserInput(), snapshot.retrievalGapType(), snapshot.allowedFocusTypes());
    }

    /**
     * 校验工具返回的追问候选是否符合UI约束：必须恰好2个选项、2个focusType
     * @param candidate 工具返回追问候选
     * @return true合法 false不合法
     */
    private boolean isValidFollowupCandidate(FollowupOptionsResult candidate) {
        return candidate.options() != null
                && candidate.focusTypes() != null
                && candidate.options().size() == 2
                && candidate.focusTypes().size() == 2;
    }

    /**
     * 兜底构建追问建议，当工具生成追问不满足条件时，后端根据缺口类型自己生成两道追问
     * @param originalUserInput 用户原始输入
     * @param gapType 检索缺口类型：时间缺失、范围缺失、主体模糊等
     * @param allowedFocusTypes 允许聚焦类型
     * @return FollowupSuggestion
     */
    private FollowupSuggestion buildFallbackSuggestion(String originalUserInput,
                                                       RetrievalGapType gapType,
                                                       List<String> allowedFocusTypes) {
        List<String> focusTypes = pickFallbackFocusTypes(gapType, allowedFocusTypes);
        List<String> options = focusTypes.stream()
                .map(focusType -> buildFallbackQuestion(originalUserInput, focusType))
                .toList();
        return new FollowupSuggestion(FOLLOWUP_PROMPT, options, "fallback");
    }

    /**
     * 根据检索缺口类型，挑选两个聚焦维度(time/procedure/subject/scope)，用于生成兜底追问
     * @param gapType 检索缺口类型
     * @param allowedFocusTypes 允许的聚焦类型
     * @return 2个focusType列表
     */
    private List<String> pickFallbackFocusTypes(RetrievalGapType gapType, List<String> allowedFocusTypes) {
        Set<String> focusTypes = new LinkedHashSet<>();
        if (allowedFocusTypes != null) {
            focusTypes.addAll(allowedFocusTypes);
        }
        // 如果可用聚焦类型不足2个，根据缺口类型补充固定组合
        if (focusTypes.size() < 2) {
            switch (gapType == null ? RetrievalGapType.MISSING_SCOPE : gapType) {
                case MISSING_TIME -> {
                    focusTypes.add("time");
                    focusTypes.add("scope");
                }
                case MISSING_PROCEDURE_BRANCH -> {
                    focusTypes.add("procedure");
                    focusTypes.add("scope");
                }
                case AMBIGUOUS_SUBJECT -> {
                    focusTypes.add("subject");
                    focusTypes.add("scope");
                }
                case NOT_IMPROVABLE, MISSING_SCOPE -> {
                    focusTypes.add("scope");
                    focusTypes.add("procedure");
                }
            }
        }
        List<String> selected = new ArrayList<>(focusTypes);
        // 兜底保底，至少两个
        if (selected.size() < 2) {
            selected = List.of("scope", "procedure");
        }
        return selected.subList(0, 2);
    }

    /**
     * 根据focusType生成兜底追问文本
     * @param originalUserInput 用户原始问题
     * @param focusType 聚焦维度 time/procedure/subject/scope
     * @return 生成的追问问句
     */
    private String buildFallbackQuestion(String originalUserInput, String focusType) {
        String question = abbreviateQuestion(originalUserInput);
        return switch (focusType == null ? "scope" : focusType.toLowerCase(Locale.ROOT)) {
            case "time" -> "针对“" + question + "”，如果进一步限定适用时间或发生阶段，知识库中有哪些明确规则？";
            case "procedure" -> "针对“" + question + "”，如果进一步限定处分程序、审批条件或复审环节，知识库中有哪些明确规则？";
            case "subject" -> "针对“" + question + "”，如果进一步限定对象、角色或学生身份，知识库中有哪些明确规则？";
            case "scope" -> "针对“" + question + "”，如果进一步限定适用对象或适用范围，知识库中有哪些明确规则？";
            default -> "针对“" + question + "”，如果进一步限定" + focusType + "，知识库中有哪些明确规则？";
        };
    }

    /**
     * 截断过长用户问题，用于追问展示，避免文案过长
     * @param originalUserInput 原始输入
     * @return 截断后简短文本
     */
    private String abbreviateQuestion(String originalUserInput) {
        if (originalUserInput == null || originalUserInput.isBlank()) {
            return "当前问题";
        }
        String trimmed = originalUserInput.trim();
        return trimmed.length() > 90 ? trimmed.substring(0, 90) + "..." : trimmed;
    }

    /**
     * 将AgentNote按照sequence序列号排序，保证UI时间线顺序正确
     * @param notes note列表
     * @return 排序后note
     */
    private List<AgentNote> sortNotes(List<AgentNote> notes) {
        return notes.stream()
                .sorted((left, right) -> Long.compare(left.sequence(), right.sequence()))
                .toList();
    }

    /**
     * 组装工具调用上下文map，传给SpringAI工具调用环境，工具内部可以读取这些参数
     * @param executionContext agent执行上下文
     * @return map上下文
     */
    private Map<String, Object> toolContext(AgentExecutionContext executionContext) {
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("requestId", executionContext.requestId());
        context.put("conversationId", executionContext.conversationId());
        context.put("msgId", executionContext.msgId());
        context.put("preselectedTags", executionContext.selectedTags());
        context.put("requestedSpaceCodes", executionContext.selectedSpaceCodes());
        return context;
    }

    /**
     * 将标签/spaceCode列表拼接为字符串，填充到prompt模板变量，给大模型阅读
     * @param values 字符串列表
     * @return 拼接后文本，空集合返回“无”
     */
    private String summarizeValues(List<String> values) {
        if (values == null || values.isEmpty()) {
            return "无";
        }
        return String.join(", ", values);
    }

    /**
     * 归一化模型输出type字段，容错LLM输出乱值，非法值降级为followup追问
     * @param type 模型输出原始type
     * @return answer / followup
     */
    private String normalizeType(String type) {
        if (type == null) {
            return "followup";
        }
        String normalized = type.trim().toLowerCase(Locale.ROOT);
        if ("answer".equals(normalized) || "followup".equals(normalized)) {
            return normalized;
        }
        return "followup";
    }

    /**
     * 归一化answerMode，容错LLM输出乱值，非法值降级normal正常回答
     * @param answerMode 模型原始输出
     * @return normal / refusal
     */
    private String normalizeAnswerMode(String answerMode) {
        if (answerMode == null) {
            return "normal";
        }
        String normalized = answerMode.trim().toLowerCase(Locale.ROOT);
        if ("normal".equals(normalized) || "refusal".equals(normalized)) {
            return normalized;
        }
        return "normal";
    }

    /**
     * 拒答模式兜底草稿，模型没有输出draftAnswer时使用默认文案
     * @param draftAnswer 模型输出草稿
     * @return 拒答文本
     */
    private String defaultRefusalDraft(String draftAnswer) {
        return (draftAnswer == null || draftAnswer.isBlank())
                ? "当前知识库中没有足够证据支持回答该问题，因此不能给出确定结论。"
                : draftAnswer;
    }

    /**
     * 拒答模式兜底finalInstruction，模型为空时填充默认指令
     * @param finalInstruction 模型输出指令
     * @return 收口指令文本
     */
    private String defaultRefusalInstruction(String finalInstruction) {
        return (finalInstruction == null || finalInstruction.isBlank())
                ? "明确说明只能依据当前知识库作答，当前证据不足，不能编造或外推。"
                : finalInstruction;
    }

    /**
     * Agent工具调用阶段系统提示词
     * 约束大模型：检索query规则、工具返回结果处理、输出严格JSON格式、字段约束、证据引用规则、权限空间约束
     */
    private String toolPhasePrompt() {
        return """
                你是校园知识库问答系统中的证据编排代理。
                知识库检索基于语义 Embedding 匹配，使用自然语言完整问句作为 query 效果最好，
                不要提取关键词、不要缩略、不要自行概括——这些操作会破坏语义向量的匹配精度。
                你的职责是：
                1. 调用 searchKnowledgeSnippets 时，query 必须使用用户的原始问题原文，不得改写。
                   仅当首次检索返回 no_result 且确实需要换一个检索角度时，才可用不同的自然语言问句重试，
                   但仍禁止使用关键词组合。
                2. 只能把工具返回 status=ok 的 items 视为候选上下文。
                3. tool_error、no_result 不能作为事实依据。
                4. 当证据不足且可通过缩小检索范围改善时，应调用 generateFollowupOptions。
                5. 当知识库本身无法回答当前问题时，直接返回 type=answer，answerMode=refusal。
                6. 你必须且只能输出一个合法 JSON 对象。
                7. 不要输出任何分析、解释、推理过程、前言、后记、Markdown 代码块。
                8. 输出必须直接从 JSON 对象开始，并直接在 JSON 对象结束处停止。
                9. 你最终输出的 JSON 只包含这些字段：
                   - type: answer 或 followup
                   - answerMode: normal 或 refusal；仅当 type=answer 时使用
                   - draftAnswer: 第二阶段使用的回答草稿，可为空
                   - finalInstruction: 第二阶段的修订或收口指令，可为空
                   - selectedEvidenceIds: 字符串数组，只能从工具 items[].citableEvidenceIds 中选择
                10. 标准输出示例如下。你必须严格按照这个 JSON 结构输出，不能增减字段，不能输出任何额外文字：
                    \\{
                      "type": "followup",
                      "answerMode": "normal",
                      "draftAnswer": "",
                      "finalInstruction": "",
                      "selectedEvidenceIds": []
                    \\}
                11. 如果 type=followup，不要在 JSON 中输出追问文案，追问候选问题由 generateFollowupOptions 工具提供。
                12. 如果 type=answer 且 answerMode=normal，selectedEvidenceIds 必须只选择真正需要的 child evidence_id 子集。
                13. 如果 type=answer 且 answerMode=refusal，也必须输出同样结构的 JSON，只是把 answerMode 设为 refusal。
                14. 不要把未选中的证据事实写入 draftAnswer 或 finalInstruction。
                15. 如果用户已经预选标签，请优先尊重这些标签。
                16. 如果用户已经限制空间范围，所有检索都只能在这些空间内继续收窄，不能越权扩大。
                17. 工具返回的 parentBlockId 只是上下文块编号，不是可引用证据；不得写入 selectedEvidenceIds。
                18. draftAnswer/finalInstruction 可以基于 parent text，但引用边界必须落到已选择的 child evidence_id。

                当前预选标签：
                {preselectedTags}

                当前预选空间：
                {preselectedSpaces}
                """;
    }
}
