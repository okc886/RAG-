package net.topikachu.rag.api;

import lombok.RequiredArgsConstructor;
import net.topikachu.rag.service.sse.SseService;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.security.Principal;

@RestController
@RequestMapping("/api/v1/sse")
@RequiredArgsConstructor // lombok自动注入SseService
public class SseController {

    private final SseService sseService;

    /**
     * GET /api/v1/sse/subscribe
     * produces = MediaType.TEXT_EVENT_STREAM_VALUE 代表这是SSE长连接响应
     * 返回 Flux<ServerSentEvent<Object>> 源源不断推送事件消息
     * @param principalMono 响应式获取登录用户信息（WebFlux不能直接拿Principal，要用Mono包装）
     * @return SSE事件流，推送该用户的ETL任务状态
     */
    @GetMapping(path = "/subscribe", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ServerSentEvent<Object>> subscribe(Mono<Principal> principalMono) {
        return principalMono
                // 如果拿不到登录用户（未登录），抛出异常 Unauthorized
                .switchIfEmpty(Mono.error(new IllegalStateException("Unauthorized for SSE")))
                // flatMapMany：把Mono(单个用户对象)转为Flux(源源不断的事件流)
                .flatMapMany(principal -> sseService.subscribe(principal.getName()));
    }
}
