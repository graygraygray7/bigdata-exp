#!/usr/bin/env bash
# ============================================================
# fetch_pages.sh —— 通用网页抓取脚本（英文网页数据采集）
#
# 把任意网页抓取下来，清理成纯英文文本，供 Hadoop WordCount 等作业使用。
# 支持 HTML 页面（去标签/脚本/导航）和纯文本/Markdown 页面。
#
# 用法:
#   ./scripts/fetch_pages.sh URL...                 # 抓 1 个或多个 URL
#   ./scripts/fetch_pages.sh -f scripts/urls.txt    # 从清单文件批量抓取
#   ./scripts/fetch_pages.sh -n bash -o webdata https://www.gnu.org/software/bash/
#
# 选项:
#   -o DIR        输出目录（默认 ./webdata）
#   -f FILE       从清单文件读取 URL，每行一条，格式: [名字] URL
#   -n NAME       为紧随其后的那个 URL 指定输出文件名（不带 .txt）
#   --force       覆盖已存在的输出文件（默认跳过重复抓取）
#   --min-len N   正文少于 N 个字符的页面丢弃（默认 200）
#   -h            显示帮助
#
# 依赖: curl、python3（仅标准库）
# ============================================================
set -euo pipefail

OUT_DIR="webdata"
URL_FILE=""
NAME=""
FORCE=0
MIN_LEN=200
UA="Mozilla/5.0 (X11; Linux x86_64) fetch_pages.sh/1.0"

usage() { sed -n '2,20p' "$0"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    -o) OUT_DIR="$2"; shift 2 ;;
    -f) URL_FILE="$2"; shift 2 ;;
    -n) NAME="$2"; shift 2 ;;
    --force) FORCE=1; shift ;;
    --min-len) MIN_LEN="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    -*) echo "未知选项: $1" >&2; usage >&2; exit 1 ;;
    *)  break ;;
  esac
done

mkdir -p "$OUT_DIR"

# ---- URL 队列: 支持命令行 URL 和清单文件两种来源 ----
declare -a URLS=()
declare -a TAGS=()   # 与 URLS 一一对应的输出名（可为空）

if [[ -n "$URL_FILE" ]]; then
  while read -r line; do
    line="${line%%#*}"                       # 去行内注释
    [[ -z "${line//[[:space:]]/}" ]] && continue
    if [[ "$line" =~ ^[A-Za-z0-9._-]+[[:space:]]+https?:// ]]; then
      TAGS+=( "${line%%[[:space:]]*}" ); URLS+=( "${line#*[[:space:]]}" )
    else
      TAGS+=( "" ); URLS+=( "$(echo "$line" | xargs)" )
    fi
  done < "$URL_FILE"
fi
# 命令行 URL 追加到队尾（-n 只作用于第一个命令行 URL）
for u in "$@"; do URLS+=( "$u" ); TAGS+=( "" ); done
if [[ -n "$NAME" && $# -gt 0 ]]; then TAGS[ ${#URLS[@]} - $# ]="$NAME"; fi

[[ ${#URLS[@]} -eq 0 ]] && { echo "没有提供 URL，用 -h 查看用法" >&2; exit 1; }

# ---- 从 URL 生成输出文件名（slug）----
slug_from_url() {
  local u="$1" s
  s=$(echo "$u" | sed -E 's~https?://~~; s/[?#].*$//; s/\.html?$//i; s/[^A-Za-z0-9]+/-/g; s/^-+|-+$//g; s/(^-{2,})/-/g' | cut -c1-48)
  [[ -z "$s" ]] && s="page"
  echo "${s,,}"
}

# ---- 清理 HTML/Markdown -> 纯文本 ----
# 注意: python 代码经 heredoc 传入（占用 stdin），数据必须走文件参数
clean_file() {
  local datafile="$1"
  python3 - "$datafile" "$MIN_LEN" <<'PY'
import sys, re, html as h
from html.parser import HTMLParser

datafile, min_len = sys.argv[1], int(sys.argv[2])
with open(datafile, encoding='utf-8', errors='replace') as f:
    raw = f.read()

# 内容形态判断：没有 HTML 标签则按 Markdown/纯文本清理
if not re.search(r'<(html|body|div|p|h\d|meta)\b', raw, re.I):
    t = raw
    t = re.sub(r'```.*?```', ' ', t, flags=re.S)          # 代码块
    t = re.sub(r'`([^`]*)`', r'\1', t)                    # 行内代码
    t = re.sub(r'!\[.*?\]\(.*?\)', ' ', t)                # 图片
    t = re.sub(r'\[([^\]]*)\]\([^)]*\)', r'\1', t)        # 链接留文字
    t = re.sub(r'^\s{0,3}#{1,6}\s+', '', t, flags=re.M)   # 标题井号
    t = re.sub(r'^\s*\|?[\s:|-]+\|?\s*$', '', t, flags=re.M)  # 表格分隔行
    t = re.sub(r'\s+\[\d+\]\s*', ' ', t)                  # 引用标记 [1]
    t = re.sub(r'\*\*?([^*]+)\*\*?', r'\1', t)            # 加粗斜体
    is_html = False
else:
    DROP = {'script','style','noscript','template','svg','head','nav','footer',
            'aside','form','iframe','button','select','option','input','textarea',
            'video','audio','object','embed'}
    # HTML5 void 元素没有结束标签，绝不能计入 skip，否则会"跳死"后面全部正文
    VOID = {'area','base','br','col','embed','hr','img','input','link','meta',
            'param','source','track','wbr'}
    class Ext(HTMLParser):
        def __init__(self):
            super().__init__(convert_charrefs=True)
            self.parts=[]; self.skip=0
        def handle_starttag(self, tag, attrs):
            if tag in VOID: return
            if tag in DROP: self.skip += 1
            elif tag in ('p','div','br','li','tr','h1','h2','h3','h4','h5','h6',
                         'pre','blockquote','table','ul','ol','section','article',
                         'dl','dt','dd','figure','figcaption','td','th'):
                self.parts.append('\n')
        def handle_endtag(self, tag):
            if tag in DROP: self.skip = max(0, self.skip-1)
            elif tag in ('p','div','li','tr','h1','h2','h3','h4','h5','h6',
                         'pre','blockquote','td','th','dt','dd'):
                self.parts.append('\n')
        def handle_data(self, data):
            if not self.skip: self.parts.append(data)
    p = Ext(); p.feed(raw)
    t = ''.join(p.parts)
    is_html = True

t = h.unescape(t) if is_html else t
t = t.replace('\r','')
t = re.sub(r'[ \t]+', ' ', t)                 # 压缩空白
t = re.sub(r' ?\n ?', '\n', t)
t = re.sub(r'\n{3,}', '\n\n', t)              # 压缩空行
t = re.sub(r'^[^A-Za-z\n]*$', '', t, flags=re.M)  # 删纯符号/数字行
t = re.sub(r'\n{2,}', '\n', t)
t = t.strip()
# 统计"像英文单词"的内容量，过短则判定为抓取失败
words = re.findall(r'[A-Za-z]+', t)
if len(words) < min_len:
    sys.stderr.write(f"CLEAN_SHORT\t{len(words)}\n")
    sys.exit(2)
sys.stdout.write(t + "\n")
PY
}

# ---- 主循环 ----
OK=0; SKIP=0; FAIL=0
for i in "${!URLS[@]}"; do
  url="${URLS[$i]}"; tag="${TAGS[$i]}"
  name="${tag:-$(slug_from_url "$url")}"
  out_file="$OUT_DIR/$name.txt"

  if [[ -s "$out_file" && $FORCE -eq 0 ]]; then
    echo "跳过(已存在): $out_file"; SKIP=$((SKIP+1)); continue
  fi

  tmp=$(mktemp)
  if ! curl -sL --compressed --max-time 30 --retry 2 --retry-delay 1 \
        -A "$UA" -o "$tmp" "$url"; then
    echo "失败(网络): $url"; rm -f "$tmp"; FAIL=$((FAIL+1)); continue
  fi

  if out=$(clean_file "$tmp"); then
    printf '%s\n\nURL: %s\n' "$out" "$url" > "$out_file"
    n=$(wc -w < "$out_file")
    echo "完成: $out_file  (${n} words)"
    OK=$((OK+1))
  else
    rc=$?
    [[ $rc -eq 2 ]] && echo "丢弃(正文过短): $url" || echo "清理失败: $url"
    FAIL=$((FAIL+1))
  fi
  rm -f "$tmp"
done

echo "----------------------------------------"
echo "汇总: 成功 $OK  跳过 $SKIP  失败 $FAIL  ->  $OUT_DIR/"
