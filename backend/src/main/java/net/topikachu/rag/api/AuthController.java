package net.topikachu.rag.api;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.topikachu.rag.common.AjaxResult;
import net.topikachu.rag.service.SysUserService;
import net.topikachu.rag.service.TokenService;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.security.Principal;
import java.time.Instant;
import java.util.Map;

/**
 * 认证授权控制器
 * WebFlux响应式接口，处理登录、注册、改密码、token刷新、用户偏好设置
 * 接口前缀：/api/v1/auth
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor // lombok 根据final字段自动生成构造函数，实现依赖注入
public class AuthController {

    private final TokenService tokenService;      // JWT令牌服务，生成access_token、refresh_token
    private final SysUserService sysUserService;  // 用户业务服务，底层JDBC阻塞数据库操作
    private final JwtDecoder jwtDecoder;          // JWT解码工具，校验签名、解析jwt内容

    /**
     * 用户登录接口，公开接口，不需要鉴权
     * @param body 请求体 {username:"xxx",password:"xxx"}
     * @return Mono<AjaxResult> 返回access_token + refresh_token令牌对
     */
    @PostMapping("/login")
    public Mono<AjaxResult> login(@RequestBody Map<String, String> body) {
        String username = body.get("username");
        String password = body.get("password");

        // 参数校验：用户名密码不能为空
        if (!StringUtils.hasText(username) || !StringUtils.hasText(password)) {
            return Mono.just(AjaxResult.error(400, "Username and password are required"));
        }

        log.info("Login attempt: username={}", username);

        // sysUserService.login 是阻塞JDBC调用
        // Mono.fromCallable：把同步阻塞方法包装成响应式Mono
        // subscribeOn(Schedulers.boundedElastic())：切换到弹性线程池执行，禁止阻塞Netty事件循环
        return Mono.fromCallable(() -> sysUserService.login(username, password))
                .subscribeOn(Schedulers.boundedElastic())
                .map(AjaxResult::success) // 登录成功，包装为成功返回体
                .doOnError(e -> log.error("Login failed for user={}: {}", username, e.getMessage()))
                .onErrorResume(e -> Mono.just(AjaxResult.error(401, e.getMessage()))); // 账号密码错误捕获异常返回401
    }

    /**
     * 用户注册接口，仅管理员可以调用，不开放自助注册
     * @PreAuthorize("hasRole('ADMIN')") SpringSecurity权限注解，要求ADMIN角色
     * @param body 请求体 {username:"xxx",password:"xxx"}
     * @return Mono<AjaxResult>
     */
    @PostMapping("/register")
    @PreAuthorize("hasRole('ADMIN')")
    public Mono<AjaxResult> register(@RequestBody Map<String, String> body) {
        String username = body.get("username");
        String password = body.get("password");

        // 参数校验
        if (!StringUtils.hasText(username) || !StringUtils.hasText(password)) {
            return Mono.just(AjaxResult.error(400, "Username and password are required"));
        }

        // Mono.fromRunnable：无返回值的阻塞方法包装，执行用户注册插入数据库
        return Mono.fromRunnable(() -> sysUserService.register(username, password))
                .subscribeOn(Schedulers.boundedElastic())
                .thenReturn(AjaxResult.success("User registered successfully", null))
                .onErrorResume(e -> Mono.just(AjaxResult.error(400, e.getMessage())));
    }

    /**
     * 修改密码接口，登录用户可用 USER / ADMIN
     * @param body 请求体 {username:"",old_password:"",new_password:""}
     * @param principalMono WebFlux自动注入，当前登录用户主体
     * @return Mono<AjaxResult>
     */
    @PostMapping("/change-password")
    @PreAuthorize("hasAnyRole('USER', 'ADMIN')")
    public Mono<AjaxResult> changePassword(@RequestBody Map<String, String> body, Mono<Principal> principalMono) {
        // switchIfEmpty：没有登录用户直接抛未授权异常
        return principalMono.switchIfEmpty(Mono.error(new IllegalStateException("Unauthorized")))
                .flatMap(principal -> {
                    // 获取当前登录用户名
                    String username = principal.getName();
                    String bodyUsername = body.get("username");
                    String oldPassword = body.get("old_password");
                    String newPassword = body.get("new_password");

                    // 安全校验：请求体携带username，必须和当前登录用户一致，防止越权修改别人密码
                    if (StringUtils.hasText(bodyUsername) && !username.equals(bodyUsername)) {
                        return Mono.just(AjaxResult.error(403, "Username mismatch with current user"));
                    }
                    // 校验新旧密码不为空
                    if (!StringUtils.hasText(oldPassword) || !StringUtils.hasText(newPassword)) {
                        return Mono.just(AjaxResult.error(400, "old_password and new_password are required"));
                    }

                    // 调用阻塞JDBC修改密码，放到boundedElastic线程池
                    return Mono.fromRunnable(() -> sysUserService.changePassword(username, oldPassword, newPassword))
                            .subscribeOn(Schedulers.boundedElastic())
                            .thenReturn(AjaxResult.success("Password changed successfully", null))
                            .onErrorResume(e -> Mono.just(AjaxResult.error(400, e.getMessage())));
                })
                .onErrorResume(e -> Mono.just(AjaxResult.error(401, e.getMessage())));
    }

    /**
     * 获取用户偏好配置
     * defaultSpaceCode：用户默认知识空间，聊天接口会用该spaceCode做知识库过滤
     * @param principalMono 当前登录用户
     * @return Mono<AjaxResult> 返回defaultSpaceCode
     */
    @GetMapping("/me/preferences")
    @PreAuthorize("hasAnyRole('USER', 'ADMIN')")
    public Mono<AjaxResult> getPreferences(Mono<Principal> principalMono) {
        return principalMono.switchIfEmpty(Mono.error(new IllegalStateException("Unauthorized")))
                .flatMap(principal -> Mono.fromCallable(() -> {
                            // 根据登录用户名查询数据库用户信息（阻塞JDBC）
                            var user = sysUserService.findByUsername(principal.getName());
                            if (user == null) {
                                throw new IllegalArgumentException("User not found");
                            }
                            // 如果数据库字段为null，返回空字符串给前端
                            return AjaxResult.success(Map.of(
                                    "defaultSpaceCode",
                                    user.getDefaultSpaceCode() == null ? "" : user.getDefaultSpaceCode()));
                        })
                        .subscribeOn(Schedulers.boundedElastic()))
                .onErrorResume(e -> Mono.just(AjaxResult.error(401, e.getMessage())));
    }

    /**
     * 更新用户偏好配置：修改默认知识空间 defaultSpaceCode
     * @param body 请求体 {"defaultSpaceCode":"xxx"}
     * @param principalMono 当前登录用户
     * @return Mono<AjaxResult>
     */
    @PutMapping("/me/preferences")
    @PreAuthorize("hasAnyRole('USER', 'ADMIN')")
    public Mono<AjaxResult> updatePreferences(@RequestBody Map<String, String> body, Mono<Principal> principalMono) {
        return principalMono.switchIfEmpty(Mono.error(new IllegalStateException("Unauthorized")))
                .flatMap(principal -> Mono.fromRunnable(() -> {
                            var user = sysUserService.findByUsername(principal.getName());
                            if (user == null) {
                                throw new IllegalArgumentException("User not found");
                            }
                            // 更新数据库用户表 default_space_code 字段
                            sysUserService.updateDefaultSpace(user.getId(), body.get("defaultSpaceCode"));
                        })
                        .subscribeOn(Schedulers.boundedElastic())
                        .thenReturn(AjaxResult.success("Preferences updated successfully", null)))
                .onErrorResume(e -> Mono.just(AjaxResult.error(400, e.getMessage())));
    }

    /**
     * token刷新接口，公开接口，不需要SpringSecurity自动鉴权
     * 业务：前端携带refresh_token，换取全新access_token + refresh_token
     * access_token短时效，refresh_token长时效
     * @param authHeader 请求头 Authorization: Bearer xxx，优先在这里取refresh_token
     * @param body 请求体，兼容模式，可以传refresh_token
     * @return Mono<AjaxResult> 返回新的令牌对
     */
    @PostMapping("/refresh")
    public Mono<AjaxResult> refresh(@RequestHeader(value = "Authorization", required = false) String authHeader,
                                    @RequestBody(required = false) Map<String, String> body) {
        // 双来源获取 refresh token：同时支持 Authorization header 和 request body，header 优先出于日志安全
        String refreshToken = null;
        // 1.优先从http请求头获取refresh_token
        if (StringUtils.hasText(authHeader) && authHeader.startsWith("Bearer ")) {
            refreshToken = authHeader.substring(7); // 截取掉 "Bearer " 7个字符前缀
        }
        // 2.header没有拿到token，再去请求体里面读取refresh_token字段，做兼容
        if (!StringUtils.hasText(refreshToken) && body != null) {
            refreshToken = body.get("refresh_token");
        }
        // header、body都没有携带refresh_token，参数错误返回400
        if (!StringUtils.hasText(refreshToken)) {
            return Mono.just(AjaxResult.error(400, "Refresh token is required"));
        }

        final String token = refreshToken; // lambda需要final变量，赋值给final临时变量
        // 手动解码 JWT 而非依赖 Spring Security 过滤器：refresh token 的 claim 结构和过期规则与 access token 不同
        // 不走security过滤器，不构建登录上下文，仅单纯校验refresh_token这张换票凭证
        return Mono.fromCallable(() -> {
                    // jwtDecoder.decode：校验JWT签名，解析得到Jwt对象，不会生成Security上下文
                    Jwt jwt = jwtDecoder.decode(token);
                    // 校验refresh_token是否过期
                    if (jwt.getExpiresAt() != null && jwt.getExpiresAt().isBefore(Instant.now())) {
                        throw new IllegalArgumentException("Refresh token expired");
                    }
                    // 校验jwt自定义claim type必须等于refresh，防止拿access_token来调用刷新接口
                    if (!"refresh".equals(jwt.getClaim("type"))) {
                        throw new IllegalArgumentException("Invalid token type");
                    }
                    // 获取jwt中subject存储的用户名
                    String username = jwt.getSubject();
                    // 查询数据库，确认该用户真实存在；防止用户已经被删除，但refresh_token还没过期的情况
                    var user = sysUserService.findByUsername(username);
                    if (user == null) {
                        throw new IllegalArgumentException("User not found");
                    }
                    // 全部校验通过，生成全新一对access_token + refresh_token返回
                    return tokenService.generateTokens(user);
                })
                .subscribeOn(Schedulers.boundedElastic()) // findByUsername是JDBC阻塞，切换弹性线程池
                .map(AjaxResult::success) // 包装成功返回结果
                // 捕获所有异常：token篡改、过期、类型错误、用户不存在，统一返回401
                .onErrorResume(e -> Mono.just(AjaxResult.error(401, "Invalid refresh token: " + e.getMessage())));
    }
}
