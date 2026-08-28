package net.topikachu.rag.business.document.access;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import net.topikachu.rag.auth.CurrentUserContext;
import net.topikachu.rag.auth.SearchScope;
import net.topikachu.rag.business.document.entity.Document;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * 知识文档访问权限策略
 * 统一封装两套权限过滤逻辑：
 * 1. MySQL文档列表查询的SQL权限过滤
 * 2. Milvus向量检索时的metadata过滤表达式生成
 * 文档ACL维度：公开文档、角色白名单、部门白名单、归属部门、知识空间、标签
 */
@Component
public class KnowledgeAccessPolicy {

    // ========== metadata字段常量（MySQL Document表 JSON字段 / Milvus metadata内的key） ==========
    public static final String SPACE_CODE = "space_code";
    public static final String TAGS = "tags";
    public static final String IS_PUBLIC = "is_public";
    public static final String ALLOWED_ROLES = "allowed_roles";
    public static final String ALLOWED_DEPT_IDS = "allowed_dept_ids";
    public static final String OWNER_DEPT_ID = "owner_dept_id";
    public static final String ACL_VERSION = "acl_version";
    public static final String CHUNK_SCHEMA_VERSION = "chunk_schema_version";

    /**
     * 构建MySQL查询Document文档的QueryWrapper
     * 组合：权限控制 + 知识空间过滤 + 标签过滤
     * @param currentUserContext 当前登录用户上下文
     * @param searchScope 前端传入检索范围（空间、标签）
     * @return MyBatisPlus查询条件包装器
     */
    public QueryWrapper<Document> buildSqlDocumentQuery(CurrentUserContext currentUserContext, SearchScope searchScope) {
        QueryWrapper<Document> query = new QueryWrapper<>();
        // 应用SQL层访问权限
        applySqlAccessControl(query, currentUserContext);
        // 应用知识空间范围过滤
        applySqlSpaceScope(query, searchScope);
        // 应用标签范围过滤
        applySqlTagScope(query, searchScope == null ? List.of() : searchScope.requestedTags());
        return query;
    }

    /**
     * 单篇文档权限判断：内存判断，不查数据库
     * 判断用户是否有权访问某一个Document实体
     * @param currentUserContext 当前用户
     * @param doc 待校验文档实体
     * @return true允许访问，false禁止
     */
    public boolean canAccessDocument(CurrentUserContext currentUserContext, Document doc) {
        // 文档为空直接无权限
        if (doc == null) {
            return false;
        }
        // 用户为空 或者是管理员，直接放行
        if (currentUserContext == null || currentUserContext.isAdmin()) {
            return true;
        }
        // 文档是公开文档，所有人可读
        if (Boolean.TRUE.equals(doc.getIsPublic())) {
            return true;
        }
        // 用户角色在文档允许角色白名单内
        if (StringUtils.hasText(currentUserContext.role())
                && doc.getAllowedRoles() != null
                && doc.getAllowedRoles().contains(currentUserContext.role())) {
            return true;
        }
        // 用户部门有效
        if (StringUtils.hasText(currentUserContext.deptId())) {
            // 用户部门等于文档归属部门，拥有权限
            if (currentUserContext.deptId().equals(doc.getOwnerDeptId())) {
                return true;
            }
            // 用户部门在文档允许访问的部门白名单
            return doc.getAllowedDeptIds() != null
                    && doc.getAllowedDeptIds().contains(currentUserContext.deptId());
        }
        // 以上条件全部不满足 → 无访问权限
        return false;
    }

    /**
     * 生成Milvus检索过滤表达式
     * 用于向量查询时过滤没有权限的chunk向量，直接在Milvus侧过滤，不用查出后内存过滤
     * @param currentUserContext 当前用户上下文
     * @param searchScope 请求检索范围（知识空间、标签）
     * @param chunkSchemaVersion 要求chunk元数据schema版本，过滤掉旧版本不兼容切片
     * @return Milvus filter条件字符串，多个条件AND连接
     */
    public String buildMilvusFilterExpr(CurrentUserContext currentUserContext, SearchScope searchScope,
                                        int chunkSchemaVersion) {
        List<String> clauses = new java.util.ArrayList<>();
        // 只取对应schema版本的chunk，过滤老版本不兼容元数据
        clauses.add("metadata[\"" + CHUNK_SCHEMA_VERSION + "\"] == " + chunkSchemaVersion);
        // 构建访问权限过滤条件
        String accessClause = buildMilvusAccessClause(currentUserContext);
        if (StringUtils.hasText(accessClause)) {
            clauses.add("(" + accessClause + ")");
        }
        // 构建知识空间过滤条件
        String spaceClause = buildMilvusSpaceClause(searchScope);
        if (StringUtils.hasText(spaceClause)) {
            clauses.add("(" + spaceClause + ")");
        }
        // 构建标签过滤条件
        String tagClause = buildMilvusTagClause(searchScope == null ? List.of() : searchScope.requestedTags());
        if (StringUtils.hasText(tagClause)) {
            clauses.add("(" + tagClause + ")");
        }
        // 全部条件 AND拼接返回给Milvus做filter
        return String.join(" AND ", clauses);
    }

    /**
     * 构建存入Milvus chunk里面的metadata元数据Map
     * ETL切片时调用，把文档ACL权限、空间、标签写入向量块元数据
     * @param storedDocument MySQL中的文档对象
     * @param effectiveTags 生效标签集合
     * @param aclVersion ACL版本号，用于权限更新任务识别是否需要刷新
     * @return 组装完成的元数据map，存入Milvus metadata字段
     */
    public Map<String, Object> buildChunkAccessMetadata(Document storedDocument,
                                                        List<String> effectiveTags,
                                                        Integer aclVersion) {
        Map<String, Object> metadata = new LinkedHashMap<>();
        // 设置知识空间编码
        metadata.put(SPACE_CODE, resolveSpaceCode(storedDocument));
        // 设置文档归属部门
        if (storedDocument != null && StringUtils.hasText(storedDocument.getOwnerDeptId())) {
            metadata.put(OWNER_DEPT_ID, storedDocument.getOwnerDeptId().trim());
        }
        // 允许访问角色列表，空则空集合
        metadata.put(ALLOWED_ROLES, storedDocument == null || storedDocument.getAllowedRoles() == null
                ? List.of()
                : normalizeStringList(storedDocument.getAllowedRoles()));
        // 允许访问部门列表
        metadata.put(ALLOWED_DEPT_IDS, storedDocument == null || storedDocument.getAllowedDeptIds() == null
                ? List.of()
                : normalizeStringList(storedDocument.getAllowedDeptIds()));
        // 是否公开文档
        metadata.put(IS_PUBLIC, storedDocument == null || Boolean.TRUE.equals(storedDocument.getIsPublic()));
        // ACL版本，修改文档权限后版本+1，用于异步刷新Milvus任务识别
        metadata.put(ACL_VERSION, aclVersion == null ? 1 : aclVersion);

        // 标签写入元数据
        List<String> normalizedTags = normalizeStringList(effectiveTags);
        if (!normalizedTags.isEmpty()) {
            metadata.put(TAGS, normalizedTags);
        }
        return metadata;
    }

    /**
     * 权限列表标准化，空集合返回null
     */
    public List<String> normalizePermissionList(List<String> values) {
        List<String> normalized = normalizeStringList(values);
        return normalized.isEmpty() ? null : List.copyOf(new LinkedHashSet<>(normalized));
    }

    /**
     * 字符串列表标准化：去空、trim、去重
     * @param values 原始字符串列表
     * @return 清洗后不可变List，null输入返回空List
     */
    public List<String> normalizeStringList(List<String> values) {
        if (values == null) {
            return List.of();
        }
        return values.stream()
                .filter(StringUtils::hasText)
                .map(String::trim)
                .distinct()
                .toList();
    }

    /**
     * MyBatisPlus QueryWrapper追加用户访问权限条件
     * 非管理员生效：公开文档 OR 角色匹配 OR 部门白名单 OR 归属部门
     * @param query 查询包装器
     * @param currentUserContext 当前用户
     */
    private void applySqlAccessControl(QueryWrapper<Document> query, CurrentUserContext currentUserContext) {
        // 管理员不做权限过滤
        if (currentUserContext == null || currentUserContext.isAdmin()) {
            return;
        }
        // 嵌套or条件：满足任意一条即可访问
        query.and(wrapper -> {
            // 条件1：文档公开
            wrapper.eq(IS_PUBLIC, 1);
            // 条件2：角色命中白名单，MySQL JSON_CONTAINS查询JSON数组
            if (StringUtils.hasText(currentUserContext.role())) {
                wrapper.or().apply("JSON_CONTAINS(" + ALLOWED_ROLES + ", JSON_QUOTE({0}))", currentUserContext.role());
            }
            // 条件3：部门在允许部门；条件4：是文档归属部门
            if (StringUtils.hasText(currentUserContext.deptId())) {
                wrapper.or().apply("JSON_CONTAINS(" + ALLOWED_DEPT_IDS + ", JSON_QUOTE({0}))", currentUserContext.deptId());
                wrapper.or().eq(OWNER_DEPT_ID, currentUserContext.deptId());
            }
        });
    }

    /**
     * SQL层：知识空间范围过滤，多个spaceCode是OR关系
     */
    private void applySqlSpaceScope(QueryWrapper<Document> query, SearchScope searchScope) {
        List<String> spaceCodes = normalizeStringList(searchScope == null ? List.of() : searchScope.requestedSpaceCodes());
        if (spaceCodes.isEmpty()) {
            return;
        }
        query.and(wrapper -> {
            boolean first = true;
            for (String spaceCode : spaceCodes) {
                if (first) {
                    wrapper.eq(SPACE_CODE, spaceCode);
                    first = false;
                } else {
                    wrapper.or().eq(SPACE_CODE, spaceCode);
                }
            }
        });
    }

    /**
     * SQL层：标签过滤，命中任意标签即可
     */
    private void applySqlTagScope(QueryWrapper<Document> query, List<String> requestedTags) {
        List<String> tags = normalizeStringList(requestedTags);
        if (tags.isEmpty()) {
            return;
        }
        query.and(wrapper -> {
            boolean first = true;
            for (String tag : tags) {
                if (first) {
                    wrapper.apply("JSON_CONTAINS(" + TAGS + ", JSON_QUOTE({0}))", tag);
                    first = false;
                } else {
                    wrapper.or().apply("JSON_CONTAINS(" + TAGS + ", JSON_QUOTE({0}))", tag);
                }
            }
        });
    }

    /**
     * 生成Milvus过滤条件：访问权限条件（公开/角色/部门/归属部门 OR关系）
     * @param currentUserContext 当前用户
     * @return Milvus filter片段，管理员返回null不追加条件
     */
    private String buildMilvusAccessClause(CurrentUserContext currentUserContext) {
        if (currentUserContext == null || currentUserContext.isAdmin()) {
            return null;
        }

        List<String> allowClauses = new java.util.ArrayList<>();
        // 条件：文档公开
        allowClauses.add(metadataField(IS_PUBLIC) + " == true");

        // 用户角色命中白名单
        if (StringUtils.hasText(currentUserContext.role())) {
            allowClauses.add("JSON_CONTAINS(" + metadataField(ALLOWED_ROLES) + ", "
                    + toJsonStringLiteral(currentUserContext.role()) + ")");
        }
        // 用户部门：部门白名单 / 归属部门
        if (StringUtils.hasText(currentUserContext.deptId())) {
            String deptLiteral = quoteStringLiteral(currentUserContext.deptId());
            allowClauses.add("JSON_CONTAINS(" + metadataField(ALLOWED_DEPT_IDS) + ", "
                    + toJsonStringLiteral(currentUserContext.deptId()) + ")");
            allowClauses.add(metadataField(OWNER_DEPT_ID) + " == " + deptLiteral);
        }
        // 多个允许条件 OR连接
        return String.join(" OR ", allowClauses);
    }

    /**
     * Milvus过滤片段：知识空间条件，多个spaceCode OR
     */
    private String buildMilvusSpaceClause(SearchScope searchScope) {
        List<String> spaceCodes = normalizeStringList(searchScope == null ? List.of() : searchScope.requestedSpaceCodes());
        if (spaceCodes.isEmpty()) {
            return null;
        }
        return spaceCodes.stream()
                .map(spaceCode -> metadataField(SPACE_CODE) + " == " + quoteStringLiteral(spaceCode))
                .reduce((left, right) -> left + " OR " + right)
                .orElse(null);
    }

    /**
     * Milvus过滤片段：标签条件，命中任意标签即可
     */
    private String buildMilvusTagClause(List<String> filterTags) {
        List<String> tags = normalizeStringList(filterTags);
        if (tags.isEmpty()) {
            return null;
        }
        return tags.stream()
                .map(tag -> "JSON_CONTAINS(" + metadataField(TAGS) + ", " + toJsonStringLiteral(tag) + ")")
                .reduce((left, right) -> left + " OR " + right)
                .orElse(null);
    }

    /**
     * 工具：生成 metadata["key"] Milvus JSON访问语法
     */
    private String metadataField(String fieldName) {
        return "metadata[\"" + fieldName + "\"]";
    }

    /**
     * 获取文档spaceCode，为空默认返回public公共空间
     */
    private String resolveSpaceCode(Document storedDocument) {
        if (storedDocument == null || !StringUtils.hasText(storedDocument.getSpaceCode())) {
            return "public";
        }
        return storedDocument.getSpaceCode().trim();
    }

    /**
     * Milvus filter JSON字符串字面量
     */
    private String toJsonStringLiteral(String value) {
        return "\"" + escapeFilterLiteral(value) + "\"";
    }

    /**
     * Milvus filter引号字符串字面量
     */
    private String quoteStringLiteral(String value) {
        return "\"" + escapeFilterLiteral(value) + "\"";
    }

    /**
     * Milvus filter字符串转义：转义反斜杠、双引号，防止表达式注入语法错误
     */
    private String escapeFilterLiteral(String value) {
        if (value == null) {
            return "";
        }
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
