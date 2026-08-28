package net.topikachu.rag.auth;

import org.springframework.util.StringUtils;

import java.util.List;

/**
 * RAG检索作用域，记录用户请求的知识库空间编码、标签条件
 * record特性：所有成员final，对象不可变；自动生成构造器、getter、equals、hashCode
 * 注意：本类仅做入参清洗，【不做权限校验】，权限裁剪需要上层业务完成
 * @param requestedSpaceCodes 用户希望查询的知识库空间编码集合（清洗后）
 * @param requestedTags 用户希望过滤的文档标签集合（清洗后）
 */
public record SearchScope(
        List<String> requestedSpaceCodes,
        List<String> requestedTags
) {

    /**
     * record紧凑构造器，new对象时自动执行
     * 对传入的空间编码、标签执行归一化清洗，不管前端传入什么脏数据，内部保持干净集合
     */
    public SearchScope {
        requestedSpaceCodes = normalize(requestedSpaceCodes);
        requestedTags = normalize(requestedTags);
    }

    /**
     * 创建空的检索作用域：不指定知识库空间、不指定标签
     * 业务含义：不限制查询范围，后续查询当前用户全部有权限的知识库
     * @return 空SearchScope实例
     */
    public static SearchScope empty() {
        return new SearchScope(List.of(), List.of());
    }

    /**
     * 替换标签，知识库空间编码保持不变，返回全新SearchScope对象
     * 注意：是直接覆盖原有标签，不是追加
     * @param tags 新的标签列表
     * @return 新生成的SearchScope实例
     */
    public SearchScope withRequestedTags(List<String> tags) {
        return new SearchScope(requestedSpaceCodes, tags);
    }

    /**
     * 在原有标签基础上追加额外标签，空间编码保持不变，返回全新SearchScope对象
     * 会对新增标签做归一化清洗，并且整体去重
     * @param extraTags 需要追加的标签集合
     * @return 新生成的SearchScope；如果入参为空，直接返回当前对象
     */
    public SearchScope mergeRequestedTags(List<String> extraTags) {
        // 没有额外标签，直接返回原对象，不需要新建
        if (extraTags == null || extraTags.isEmpty()) {
            return this;
        }
        return new SearchScope(
                requestedSpaceCodes,
                // 原有标签流 和 清洗后的新增标签流合并，去重转为List
                java.util.stream.Stream.concat(requestedTags.stream(), normalize(extraTags).stream())
                        .distinct()
                        .toList());
    }

    /**
     * 归一化清洗集合工具方法
     * 处理前端传入的脏参数：null、空串、全空格、前后空格、重复元素
     * @param values 原始字符串集合（前端传入）
     * @return 清洗完成后的不可变List，不会返回null
     */
    private static List<String> normalize(List<String> values) {
        // null直接返回空不可变集合
        if (values == null) {
            return List.of();
        }
        return values.stream()
                // 过滤null、空字符串、全部空格的字符串
                .filter(StringUtils::hasText)
                // 去除字符串前后空白
                .map(String::trim)
                // 去除重复项
                .distinct()
                .toList();
    }
}
