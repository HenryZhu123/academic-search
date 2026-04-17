# OpenClaw Skill: Academic Fulltext Pipeline

## 1) Skill Purpose

将用户的论文检索请求交给本项目的 Python 流水线处理：

- 从 **PubMed / bioRxiv / Semantic Scholar** 检索相关论文
- 返回相关论文摘要（abstract）给用户
- 将每篇论文的全量数据写入 PostgreSQL 的 `squai_table`

核心命令：

```bash
python scripts/integrate_fulltext_pipeline.py --query "{query}" --limit {limit}
```

---

## 2) When to Call

满足以下任一条件时，必须调用本 skill：

1. 用户明确指定要使用 `academic-search`（如："用 academic-search 查..."、"必须走 academic-search skill"）
2. 用户提出论文检索需求，且目标是某个方向/主题相关论文（如："检索多模态学习相关论文"、"找癌症早筛相关文献"）
3. 用户要求返回论文摘要、并希望结果可入库或可追踪

典型触发意图关键词（中文/英文）：

- 检索/搜索/查找/调研 + 论文/文献
- related papers / literature search / survey papers
- 某领域 "相关论文"、"最新论文"、"综述文献"

以下场景不调用本 skill：

- 仅做概念解释，不需要检索真实论文
- 用户只要求润色文本、翻译、写作，不涉及论文检索

---

## 3) Execution Contract

### Tool Input

- `query` (string, required): 用户问题或检索关键词
- `limit` (integer, optional, default `10`, range `1-100`): 返回论文数量上限

### Working Directory

执行命令时的工作目录必须是项目根目录：

`/home/ubuntu/academic-search/academic-search-main`

### Environment Variables

数据库配置（可选，未配置时使用脚本默认值）：

- `SQUAI_DB_NAME`
- `SQUAI_DB_USER`
- `SQUAI_DB_PASSWORD`
- `SQUAI_DB_HOST`
- `SQUAI_DB_PORT`



---

## 4) Runtime Behavior

每次调用必须遵循以下流程：

1. 读取用户请求并生成 `query`
2. 若用户未指定数量，设置 `limit=10`
3. 调用 Python 脚本
4. 解析脚本 stdout 返回的 JSON 结果
5. 将摘要结果总结后展示给用户

---

## 5) Expected Script Output (JSON)

脚本返回 JSON，结构如下：

```json
{
  "query": "single-cell RNA sequencing",
  "count": 10,
  "abstracts": [
    {
      "paper_id": "doi:10.xxxx/xxxx",
      "title": "Paper title",
      "abstract": "Paper abstract...",
      "year": 2025,
      "source_platforms": ["pubmed", "semanticscholar"]
    }
  ],
  "stored_paper_ids": ["doi:10.xxxx/xxxx", "pubmed:12345678"]
}
```

---

## 6) Response Policy to User

调用成功后，按以下格式回答：

1. 简要说明检索条件（query、limit）
2. 输出论文摘要列表（标题 + 年份 + 来源平台 + 摘要总结）

建议输出模板：

- 已检索：`{query}`（limit=`{limit}`）
- 命中：`{count}` 篇
- 论文列表：标题 + 年份 + 来源平台 + 摘要总结

当 `count=0` 时：

- 明确告知未检索到结果
- 给出建议（换关键词、放宽领域词、增大 `limit`）

---

## 7) Failure Handling

当脚本执行失败时：

1. 返回核心错误信息（精简 stderr）
2. 提供下一步排查建议，不要沉默失败

优先排查顺序：

1. Python 依赖是否安装（`requests`, `pypdf`, `psycopg2-binary`）
2. PostgreSQL 是否可连接
3. `squai_table` 是否已创建
4. 外网是否可访问 PubMed / bioRxiv / Semantic Scholar
5. Semantic Scholar 限流（建议配置 `S2_API_KEY`）

---

## 8) Command Examples

### Example A

```bash
python scripts/integrate_fulltext_pipeline.py --query "CRISPR gene editing" --limit 8
```

### Example B

```bash
python scripts/integrate_fulltext_pipeline.py --query "single-cell RNA sequencing cancer" --limit 12
```

---

## 9) Safety and Constraints

- 只使用公开 API，不绕过付费墙
- 不输出非结构化原始 HTML
- 输出给用户时优先摘要信息；完整数据由数据库持久化保存
- 在没有明确需求时，不额外抓取超出 `limit` 的结果

---

## 10) OpenClaw System Prompt Snippet (Optional)

可在 OpenClaw 中加入以下行为约束：

```text
Call this skill when:
- user explicitly asks to use academic-search
- user asks to retrieve/search related papers in any research topic

Execution command:
python scripts/integrate_fulltext_pipeline.py --query "{query}" --limit {limit}

Always parse JSON output and reply with:
1) count
2) abstract list (title, year, source_platforms, abstract)
3) storage status using stored_paper_ids

If execution fails, return concise error + actionable troubleshooting steps.
```
