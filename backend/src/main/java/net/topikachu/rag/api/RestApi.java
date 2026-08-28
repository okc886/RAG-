package net.topikachu.rag.api;

import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.agent.AgentChatService;
import net.topikachu.rag.auth.CurrentUserContext;
import net.topikachu.rag.auth.CurrentUserContextService;
import net.topikachu.rag.auth.SearchScope;
import net.topikachu.rag.business.document.service.DocumentService;
import net.topikachu.rag.observability.TracingSupport;
import net.topikachu.rag.service.chat.ChatService;
import net.topikachu.rag.service.chat.SourceValidationException;
import net.topikachu.rag.service.etl.EtlPipeline;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

import java.io.Serializable;
import java.security.Principal;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StringUtils;

import static org.springframework.ai.reader.tika.TikaDocumentReader.METADATA_SOURCE;

/**
 * REST API 控制器，提供聊天和文档索引功能
 * 使用 Server-Sent Events (SSE) 实现流式响应
 */
@RestController()
@RequestMapping("/api/v1")
@Slf4j
@RequiredArgsConstructor
public class RestApi {

    // 核心聊天服务，负责处理RAG模式的对话
    private final ChatService chatService;
    // Agent聊天服务，负责处理Agent模式的对话
    private final AgentChatService agentChatService;
    // 追踪支持服务，用于链路追踪和性能监控
    private final TracingSupport tracingSupport;
    // 当前用户上下文服务，用于获取用户信息
    private final CurrentUserContextService currentUserContextService;
    // 文档服务，用于处理文档相关的业务逻辑
    private final DocumentService documentService;

    // ETL管道服务，用于文档的提取、转换和加载
    private final EtlPipeline etlPipeline;

    // 是否启用Agent模式，从配置文件读取
    @Value("${rag.agent.enable:true}")
    private boolean agentEnabled;

    // Agent默认模式，从配置文件读取，默认为rag
    @Value("${rag.agent.default-mode:rag}")
    private String agentDefaultMode;

    /**
     * 聊天接口 - 使用SSE流式返回响应
     *
     * @param chatRequest 聊天请求体，包含用户输入、标签、空间编码等信息
     * @param conversationId 会话ID，用于标识对话会话
     * @param locustRunId Locust压力测试运行ID（可选）
     * @param questionId 问题ID（可选）
     * @param questionBucket 问题桶（可选）
     * @param principalMono Spring Security用户主体
     * @return Flux<ServerSentEvent<Object>> SSE事件流
     */
    @PostMapping(path = "/chat", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @PreAuthorize("hasAnyRole('USER', 'ADMIN')")  // 需要USER或ADMIN角色才能访问
    public Flux<ServerSentEvent<Object>> chat(@RequestBody ChatRequest chatRequest,
                                              @RequestParam() String conversationId,
                                              @RequestHeader(value = "X-Locust-Run-Id", required = false) String locustRunId,
                                              @RequestHeader(value = "X-Question-Id", required = false) String questionId,
                                              @RequestHeader(value = "X-Question-Bucket", required = false) String questionBucket,
                                              Mono<Principal> principalMono) {
        // 从Mono中获取用户主体，然后处理请求
        return principalMono.flatMapMany(principal -> {
            // 根据用户名解析当前用户上下文
            CurrentUserContext currentUserContext = currentUserContextService.resolveByUsername(principal.getName());

            // 构建会话键，格式为 "用户名:会话ID"，确保会话归属
            var conversationKey = conversationId.startsWith(principal.getName() + ":")
                    ? conversationId
                    : String.format("%s:%s", principal.getName(), conversationId);

            // 创建搜索范围对象，包含空间编码和标签
            SearchScope requestedScope = new SearchScope(chatRequest.spaceCodes(), chatRequest.tags());

            // 确定聊天模式（agent或rag），如果未指定则使用默认模式
            String requestedMode = chatRequest.mode();
            String mode = StringUtils.hasText(requestedMode) ? requestedMode : agentDefaultMode;

            // 判断是否使用Agent模式
            boolean useAgent = agentEnabled && "agent".equalsIgnoreCase(mode);

            // 生成消息ID，如果请求中未提供则自动生成
            String msgId = StringUtils.hasText(chatRequest.msgId()) ? chatRequest.msgId() : ("msg-" + System.currentTimeMillis());

            // 解析有效的搜索范围（考虑用户权限和默认设置）
            return documentService.resolveEffectiveSearchScope(currentUserContext, requestedScope)
                    .flatMapMany(searchScope -> {
                        // 为当前追踪添加标签信息
                        tagCurrentChatTrace(principal.getName(), conversationId, conversationKey, mode, chatRequest.modelId(),
                                locustRunId, questionId, questionBucket, searchScope);

                        // 如果Agent模式被禁用但请求了Agent模式，返回错误
                        if (!agentEnabled && "agent".equalsIgnoreCase(mode)) {
                            log.warn("Agent mode requested while disabled. conversationId={}, user={}", conversationId, principal.getName());
                            return Flux.just(errorEvent(msgId, "当前系统未启用 Agent 模式，请切换到快速模式后重试。"),
                                    doneEvent(msgId));
                        }

                        // 根据模式选择使用Agent服务或普通聊天服务
                        if (useAgent) {
                            // Agent模式：使用AgentChatService处理
                            return agentChatService.streamEvents(
                                    chatRequest.userInput(),
                                    conversationKey,
                                    currentUserContext,
                                    searchScope,
                                    chatRequest.modelId(),
                                    msgId);
                        }

                        // RAG模式：使用ChatService处理，包含来源引用
                        return chatService.streamWithSources(
                                        chatRequest.userInput(),
                                        conversationKey,
                                        currentUserContext,
                                        searchScope,
                                        chatRequest.modelId(),
                                        msgId)
                                .flatMapMany(response -> {
                                    // 将使用的来源转换为前端需要的格式
                                    List<Map<String, Object>> sourceMetadata = response.usedSources().stream()
                                            .map(source -> {
                                                Map<String, Object> item = new LinkedHashMap<>();
                                                item.put("evidenceId", source.evidenceId());
                                                item.put("doc_uuid", source.docUuid());
                                                item.put("file_name", source.fileName());
                                                item.put("page_number", source.pageNumber());
                                                item.put("file_type", source.fileType());
                                                return item;
                                            })
                                            .collect(Collectors.toList());

                                    // 构建来源事件
                                    ServerSentEvent<Object> sourceEvent = ServerSentEvent.builder()
                                            .event("sources")
                                            .data(sourceMetadata)
                                            .build();

                                    // 将响应内容转换为消息事件流
                                    Flux<ServerSentEvent<Object>> messageStream = response.flux()
                                            .map(content -> ServerSentEvent.builder()
                                                    .event("message")
                                                    .data((Object) content)
                                                    .build());

                                    // 按照顺序发送：来源事件 -> 消息流 -> 完成事件
                                    return Flux.concat(Flux.just(sourceEvent), messageStream, Flux.just(doneEvent(msgId)));
                                })
                                // 错误处理
                                .onErrorResume(exp -> {
                                    String message = "系统繁忙，请稍后重试。";
                                    // 根据不同异常类型提供不同的用户友好消息
                                    if (exp instanceof net.topikachu.rag.service.chat.RetrievalException retrievalException) {
                                        message = retrievalException.getUserMessage();
                                    } else if (exp instanceof SourceValidationException sourceValidationException) {
                                        message = sourceValidationException.getUserMessage();
                                    } else if (exp instanceof ResourceAccessException) {
                                        message = "模型服务响应超时，请稍后重试或切换模型。";
                                    }
                                    // 根据异常类型决定日志级别
                                    if (exp instanceof SourceValidationException
                                            || exp instanceof net.topikachu.rag.service.chat.RetrievalException) {
                                        log.warn("Chat request failed with user-facing error: {}", message);
                                    } else {
                                        log.error("Error in chat", exp);
                                    }
                                    // 构建错误响应
                                    ServerSentEvent<Object> fallbackEvent = ServerSentEvent.builder()
                                            .event("message")
                                            .data((Object) message)
                                            .build();
                                    if (exp instanceof SourceValidationException) {
                                        // 来源验证异常只返回消息和完成事件
                                        return Flux.just(fallbackEvent, doneEvent(msgId));
                                    }
                                    // 其他异常返回错误事件、消息事件和完成事件
                                    return Flux.just(errorEvent(msgId, message), fallbackEvent, doneEvent(msgId));
                                });
                    })
                    // 最外层错误处理
                    .onErrorResume(exp -> Flux.just(
                            errorEvent(msgId, exp.getMessage() == null ? "请先选择知识空间。" : exp.getMessage()),
                            doneEvent(msgId)));
        });
    }

    /**
     * 文档索引接口 - 触发ETL管道进行文档处理
     *
     * @return Flux<String> 处理后的文档来源信息流
     */
    @PostMapping(path = "/index", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @PreAuthorize("hasRole('ADMIN')")  // 需要ADMIN角色才能访问
    public Flux<String> index() {
        // 执行ETL管道的提取-转换-加载流程，返回每个文档的来源元数据
        return etlPipeline.ingestionFlux()
                .map(document -> (String) document.getMetadata().get(METADATA_SOURCE));
    }

    /**
     * 聊天请求记录类
     */
    record ChatRequest(String userInput, List<String> tags, List<String> spaceCodes, String modelId, String mode,
                       String msgId) implements Serializable {
    }

    /**
     * 构建错误事件的SSE
     *
     * @param msgId 消息ID
     * @param message 错误消息
     * @return ServerSentEvent<Object> 错误事件
     */
    private ServerSentEvent<Object> errorEvent(String msgId, String message) {
        return ServerSentEvent.builder()
                .event("error")
                .data((Object) Map.of("msgId", msgId, "message", message))
                .build();
    }

    /**
     * 构建完成事件的SSE
     *
     * @param msgId 消息ID
     * @return ServerSentEvent<Object> 完成事件
     */
    private ServerSentEvent<Object> doneEvent(String msgId) {
        return ServerSentEvent.builder()
                .event("done")
                .data((Object) Map.of("msgId", msgId))
                .build();
    }

    /**
     * 为当前追踪添加标签信息，用于链路追踪和监控
     *
     * @param username 用户名
     * @param conversationId 会话ID
     * @param conversationKey 会话键（用户名:会话ID）
     * @param mode 聊天模式
     * @param modelId 模型ID
     * @param locustRunId Locust运行ID
     * @param questionId 问题ID
     * @param questionBucket 问题桶
     * @param searchScope 搜索范围
     */
    private void tagCurrentChatTrace(String username,
                                     String conversationId,
                                     String conversationKey,
                                     String mode,
                                     String modelId,
                                     String locustRunId,
                                     String questionId,
                                     String questionBucket,
                                     SearchScope searchScope) {
        Map<String, Object> tags = new LinkedHashMap<>();
        tags.put("langfuse.user.id", username);                    // 用户ID（Langfuse）
        tags.put("langfuse.session.id", conversationKey);          // 会话ID（Langfuse）
        tags.put("conversation.id", conversationId);              // 会话ID
        tags.put("chat.mode", mode);                              // 聊天模式
        tags.put("chat.model_id", modelId);                       // 模型ID
        tags.put("chat.requested_spaces", searchScope == null ? "" : String.join(",", searchScope.requestedSpaceCodes()));  // 请求的空间
        tags.put("chat.requested_tags", searchScope == null ? "" : String.join(",", searchScope.requestedTags()));          // 请求的标签
        tags.put("locust.run_id", locustRunId);                   // Locust运行ID
        tags.put("question.id", questionId);                      // 问题ID
        tags.put("question.bucket", questionBucket);              // 问题桶
        tracingSupport.tagCurrent(tags);                          // 应用标签到当前追踪上下文
    }
}