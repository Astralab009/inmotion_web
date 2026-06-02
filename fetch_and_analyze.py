#!/usr/bin/env python3
"""Fetch finance news and map items to A-share industry-chain impacts."""

from __future__ import annotations

import argparse
import datetime as dt
import email.utils
import html
import json
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable


DEFAULT_SOURCES = [
    {
        "name": "Sina Finance Roll",
        "type": "html",
        "url": "https://finance.sina.com.cn/roll/",
        "enabled": True,
    }
]

DEFAULT_CHAIN_MAP = Path(__file__).resolve().parents[1] / "references" / "Industry_Chains.txt"
GENERIC_TERMS = {
    "电池",
    "芯片",
    "材料",
    "设备",
    "服务",
    "渠道",
    "消费",
    "电商",
    "存储",
    "雷达",
    "银行",
    "风险管理",
}


SEED_CHAINS = [
    {
        "chain": "Semiconductors",
        "keywords": "半导体 芯片 晶圆 封测 光刻胶 EDA GPU AI芯片 存储 DRAM NAND HBM 先进封装".split(),
        "companies": [
            ("中芯国际", "688981", "manufacturing"),
            ("北方华创", "002371", "equipment"),
            ("中微公司", "688012", "equipment"),
            ("华海清科", "688120", "equipment/CMP"),
            ("沪硅产业", "688126", "silicon wafer"),
            ("南大光电", "300346", "materials/photoresist and gases"),
            ("长电科技", "600584", "packaging/testing"),
            ("通富微电", "002156", "packaging/testing"),
            ("韦尔股份", "603501", "chip design"),
        ],
    },
    {
        "chain": "New Energy Vehicles And Batteries",
        "keywords": "新能源车 锂电 动力电池 固态电池 钠电池 电解液 隔膜 正极 负极 碳酸锂 充电桩".split(),
        "companies": [
            ("宁德时代", "300750", "battery cell/system"),
            ("亿纬锂能", "300014", "battery cell"),
            ("国轩高科", "002074", "battery cell"),
            ("天齐锂业", "002466", "lithium resource"),
            ("赣锋锂业", "002460", "lithium resource/materials"),
            ("恩捷股份", "002812", "separator"),
            ("璞泰来", "603659", "anode/materials"),
            ("天赐材料", "002709", "electrolyte"),
            ("比亚迪", "002594", "automaker/battery"),
        ],
    },
    {
        "chain": "Photovoltaics",
        "keywords": "光伏 硅料 硅片 电池片 组件 TOPCon HJT 钙钛矿 逆变器 储能".split(),
        "companies": [
            ("隆基绿能", "601012", "wafers/modules"),
            ("TCL中环", "002129", "wafers"),
            ("通威股份", "600438", "silicon/cells"),
            ("晶澳科技", "002459", "modules"),
            ("天合光能", "688599", "modules"),
            ("阳光电源", "300274", "inverter/storage"),
            ("福斯特", "603806", "PV film"),
            ("福莱特", "601865", "PV glass"),
        ],
    },
    {
        "chain": "AI And Computing Infrastructure",
        "keywords": "人工智能 大模型 算力 AI服务器 GPU服务器 数据中心 液冷 光模块 CPO 交换机 算力租赁".split(),
        "companies": [
            ("浪潮信息", "000977", "AI servers"),
            ("工业富联", "601138", "servers/manufacturing"),
            ("中际旭创", "300308", "optical modules"),
            ("新易盛", "300502", "optical modules"),
            ("天孚通信", "300394", "optical components"),
            ("沪电股份", "002463", "PCB"),
            ("紫光股份", "000938", "networking/servers"),
            ("科大讯飞", "002230", "AI applications"),
        ],
    },
    {
        "chain": "Pharmaceuticals And Biotech",
        "keywords": "创新药 CXO GLP-1 医保谈判 集采 疫苗 ADC 医疗器械 减肥药".split(),
        "companies": [
            ("恒瑞医药", "600276", "innovative drugs"),
            ("药明康德", "603259", "CXO"),
            ("凯莱英", "002821", "CDMO"),
            ("泰格医药", "300347", "clinical CRO"),
            ("迈瑞医疗", "300760", "medical devices"),
            ("爱美客", "300896", "medical aesthetics"),
        ],
    },
    {
        "chain": "Consumer And Agriculture",
        "keywords": "猪价 生猪 白酒 啤酒 乳制品 旅游 免税 黄金周 消费刺激".split(),
        "companies": [
            ("牧原股份", "002714", "hog breeding"),
            ("温氏股份", "300498", "livestock breeding"),
            ("新希望", "000876", "feed/livestock"),
            ("贵州茅台", "600519", "liquor"),
            ("五粮液", "000858", "liquor"),
            ("青岛啤酒", "600600", "beer"),
            ("中国中免", "601888", "duty-free"),
        ],
    },
]


@dataclass
class NewsItem:
    title: str
    source: str = ""
    url: str = ""
    summary: str = ""
    published_at: str = ""
    content: str = ""


@dataclass
class Company:
    name: str
    ticker: str
    role: str


@dataclass
class ChainLink:
    stage: str
    link: str
    board: str
    companies: list[Company]


@dataclass
class IndustryChain:
    name: str
    links: list[ChainLink]


def fetch_url(url: str, timeout: int = 15) -> str:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 news-to-a-share-impact/0.1",
            "Accept": "application/rss+xml, application/xml, text/html;q=0.9, */*;q=0.8",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
        return raw.decode(charset, errors="replace")


def parse_date(value: str) -> str:
    if not value:
        return ""
    try:
        parsed = email.utils.parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return value.strip()
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone().isoformat(timespec="seconds")


def strip_tags(value: str) -> str:
    value = re.sub(r"<script[\s\S]*?</script>", " ", value, flags=re.I)
    value = re.sub(r"<style[\s\S]*?</style>", " ", value, flags=re.I)
    value = re.sub(r"<[^>]+>", " ", value)
    value = html.unescape(value)
    return re.sub(r"\s+", " ", value).strip()


def parse_rss(xml_text: str, source_name: str) -> list[NewsItem]:
    root = ET.fromstring(xml_text)
    items = []
    for node in root.findall(".//item"):
        title = (node.findtext("title") or "").strip()
        if not title:
            continue
        items.append(
            NewsItem(
                title=html.unescape(title),
                source=source_name,
                url=(node.findtext("link") or "").strip(),
                summary=strip_tags(node.findtext("description") or ""),
                published_at=parse_date(node.findtext("pubDate") or ""),
            )
        )
    return items


def parse_html_titles(text: str, source_name: str, base_url: str) -> list[NewsItem]:
    items = []
    seen = set()
    for match in re.finditer(r'<a\b[^>]*href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', text, flags=re.I | re.S):
        url, label = match.groups()
        title = strip_tags(label)
        if len(title) < 8 or title in seen:
            continue
        if not url.startswith(("http://", "https://")):
            url = urllib.parse.urljoin(base_url, url)
        seen.add(title)
        items.append(NewsItem(title=title, source=source_name, url=url))
    return items


def unwrap_article_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qs(parsed.query)
    if "url" in query and query["url"]:
        return query["url"][0]
    return url


def extract_between(text: str, patterns: list[str]) -> str:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I | re.S)
        if match:
            return strip_tags(match.group(1))
    return ""


def parse_article_page(text: str, fallback_title: str, fallback_source: str) -> dict:
    title = extract_between(
        text,
        [
            r"<h1[^>]*>([\s\S]*?)</h1>",
            r"<title[^>]*>([\s\S]*?)</title>",
        ],
    )
    content = extract_between(
        text,
        [
            r'<div[^>]+id=["\']artibody["\'][^>]*>([\s\S]*?)</div>\s*(?:<div|<script|<!--)',
            r'<div[^>]+class=["\'][^"\']*(?:article|content|main-content)[^"\']*["\'][^>]*>([\s\S]*?)</div>',
            r"<article[^>]*>([\s\S]*?)</article>",
        ],
    )
    if not content:
        paragraphs = re.findall(r"<p[^>]*>([\s\S]*?)</p>", text, flags=re.I)
        content = strip_tags("\n".join(paragraphs[:80]))
    source = extract_between(text, [r"(?:来源|文章来源)[：:]\s*([^<\n]{2,40})"]) or fallback_source
    time_match = re.search(r"(20\d{2}[-年]\d{1,2}[-月]\d{1,2}[日]?\s+\d{1,2}:\d{2}(?::\d{2})?)", text)
    return {
        "title": title or fallback_title,
        "source": source,
        "published_at": time_match.group(1) if time_match else "",
        "content": content,
    }


def enrich_with_article_content(item: NewsItem) -> NewsItem:
    if not item.url:
        return item
    try:
        text = fetch_url(unwrap_article_url(item.url))
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        print(f"Article fetch failed for {item.title}: {exc}", file=sys.stderr)
        return item
    parsed = parse_article_page(text, item.title, item.source)
    return NewsItem(
        title=parsed["title"] or item.title,
        source=parsed["source"] or item.source,
        url=item.url,
        summary=item.summary,
        published_at=item.published_at or parsed["published_at"],
        content=parsed["content"],
    )


def parse_companies(value: str) -> list[Company]:
    companies = []
    for chunk in re.split(r"、", value):
        chunk = chunk.strip()
        match = re.match(r"(.+?)\(([^,，()]+)[,，]([^()]+)\)", chunk)
        if not match:
            continue
        companies.append(Company(name=match.group(1).strip(), ticker=match.group(2).strip(), role=match.group(3).strip()))
    return companies


def load_industry_chains(path: str | Path) -> list[IndustryChain]:
    text = Path(path).read_text(encoding="utf-8")
    chains: list[IndustryChain] = []
    current: IndustryChain | None = None
    stage = ""
    for raw_line in text.splitlines():
        line = raw_line.strip()
        chain_match = re.match(r"^\d+\.\s*(.+)$", line)
        if chain_match:
            current = IndustryChain(name=chain_match.group(1).strip(), links=[])
            chains.append(current)
            stage = ""
            continue
        if line in {"上游：", "中游：", "下游："}:
            stage = line.rstrip("：")
            continue
        link_match = re.match(r"^-\s*(.+?)（板块：(.+?)）→\s*(.+)$", line)
        if current and stage and link_match:
            current.links.append(
                ChainLink(
                    stage=stage,
                    link=link_match.group(1).strip(),
                    board=link_match.group(2).strip(),
                    companies=parse_companies(link_match.group(3).strip()),
                )
            )
    return chains


def load_sources(path: str | None) -> list[dict]:
    if not path:
        return DEFAULT_SOURCES
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError("source config must be a JSON array")
    return data


def fetch_sources(sources: Iterable[dict], limit: int) -> list[NewsItem]:
    items: list[NewsItem] = []
    for source in sources:
        if not source.get("enabled", True):
            continue
        name = source.get("name") or source.get("url", "unknown")
        url = source.get("url")
        source_type = source.get("type", "rss")
        if not url:
            continue
        try:
            text = fetch_url(url)
            if source_type == "rss":
                parsed = parse_rss(text, name)
            elif source_type == "html":
                parsed = parse_html_titles(text, name, url)
            else:
                print(f"Skipping unsupported source type {source_type!r} for {name}", file=sys.stderr)
                continue
            items.extend(parsed)
        except (urllib.error.URLError, TimeoutError, ET.ParseError, ValueError) as exc:
            print(f"Fetch failed for {name}: {exc}", file=sys.stderr)
    return dedupe(items)[:limit]


def load_input_json(path: str) -> list[NewsItem]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, list):
        raise ValueError("input JSON must be an array")
    return [NewsItem(**{k: v for k, v in row.items() if k in NewsItem.__dataclass_fields__}) for row in data]


def dedupe(items: Iterable[NewsItem]) -> list[NewsItem]:
    result = []
    seen = set()
    for item in items:
        key = (normalize(item.title), item.url.strip())
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def normalize(value: str) -> str:
    return re.sub(r"\s+", "", value).lower()


def infer_direction(text: str) -> str:
    negative = "下跌 下降 下滑 降价 亏损 制裁 限制 事故 停产 需求疲软 过剩 集采".split()
    positive = "上涨 增长 提价 突破 量产 扩产 中标 政策支持 国产替代 供不应求".split()
    if any(word in text for word in negative) and any(word in text for word in positive):
        return "mixed"
    if any(word in text for word in negative):
        return "negative"
    if any(word in text for word in positive):
        return "positive"
    return "watch"


def text_for_matching(item: NewsItem) -> str:
    text = f"{item.title} {item.summary} {item.content}"
    boilerplate_markers = [
        "风险提示",
        "免责声明",
        "基金有风险",
        "基金费率",
        "相关费用说明",
        "投资者在申购",
        "注：ETF基金",
    ]
    cut = len(text)
    for marker in boilerplate_markers:
        pos = text.find(marker)
        if pos > -1:
            cut = min(cut, pos)
    return text[:cut]


def score_link(text: str, title_text: str, chain: IndustryChain, link: ChainLink) -> tuple[int, list[str]]:
    terms = [chain.name, link.link, link.board]
    terms.extend(company.name for company in link.companies)
    hits = []
    score = 0
    for term in terms:
        if not term:
            continue
        if term in GENERIC_TERMS:
            continue
        if term in title_text:
            hits.append(term)
            score += 4
        elif term in text:
            hits.append(term)
            score += 2
    return score, list(dict.fromkeys(hits))


def analyze_item(item: NewsItem, per_chain: int, chains: list[IndustryChain]) -> dict:
    text = text_for_matching(item)
    title_text = item.title
    matched = []
    direction = infer_direction(text)
    for chain in chains:
        staged: dict[str, list[dict]] = {"上游": [], "中游": [], "下游": []}
        chain_hits = set()
        chain_score = 0
        for link in chain.links:
            score, hits = score_link(text, title_text, chain, link)
            if score <= 0:
                continue
            chain_score += score
            chain_hits.update(hits)
            companies = []
            for company in link.companies[:per_chain]:
                direct = company.name in text
                companies.append(
                    {
                        "name": company.name,
                        "ticker": company.ticker,
                        "role": company.role,
                        "impact_direction": direction,
                        "directness": "direct" if direct else "industry-chain",
                        "confidence": "high" if direct else "medium",
                    }
                )
            staged[link.stage].append(
                {
                    "link": link.link,
                    "board": link.board,
                    "matched_keywords": hits,
                    "companies": companies,
                }
            )
        if chain_score:
            matched.append(
                {
                    "industry": chain.name,
                    "score": chain_score,
                    "matched_keywords": sorted(chain_hits),
                    "upstream": staged["上游"],
                    "midstream": staged["中游"],
                    "downstream": staged["下游"],
                }
            )
    matched.sort(key=lambda row: row["score"], reverse=True)
    return {
        **asdict(item),
        "article_body": item.content or item.summary or "",
        "related_industries": matched,
        "match_status": "matched" if matched else "无法匹配",
        "verification_needed": [
            "Confirm the article body was parsed correctly and is not navigation or boilerplate.",
            "Confirm company exposure and materiality with latest filings and announcements.",
            "If no industry is matched, do not invent related industries outside the provided knowledge base.",
        ],
    }


def filter_recent(items: list[NewsItem], hours: int) -> list[NewsItem]:
    if hours <= 0:
        return items
    now = dt.datetime.now().astimezone()
    keep = []
    for item in items:
        if not item.published_at:
            keep.append(item)
            continue
        try:
            published = dt.datetime.fromisoformat(item.published_at)
        except ValueError:
            keep.append(item)
            continue
        if now - published <= dt.timedelta(hours=hours):
            keep.append(item)
    return keep


def render_markdown(results: list[dict]) -> str:
    lines = [
        "# A-Share News Impact Scan",
        "",
        "Research screening only; not investment advice. Verify company exposure and latest disclosures before use.",
        "",
    ]
    for index, row in enumerate(results, 1):
        lines.extend([f"## {index}. {row['title']}", "", f"- Source: {row.get('source') or 'unknown'}"])
        if row.get("published_at"):
            lines.append(f"- Published: {row['published_at']}")
        if row.get("url"):
            lines.append(f"- URL: {row['url']}")
        body = row.get("article_body") or row.get("summary") or ""
        lines.append(f"- Body: {body[:1200] if body else '(no article body parsed)'}")
        if not row["related_industries"]:
            lines.extend(["- Related industries: 无法匹配", ""])
            continue
        for industry in row["related_industries"]:
            lines.append(f"- Related industry: {industry['industry']} ({', '.join(industry['matched_keywords'][:10])})")
            for label, key in [("上游", "upstream"), ("中游", "midstream"), ("下游", "downstream")]:
                if not industry[key]:
                    continue
                lines.append(f"  - {label}:")
                for link in industry[key]:
                    companies = "、".join(f"{company['name']}({company['ticker']},{company['role']})" for company in link["companies"])
                    lines.append(f"    - {link['link']}（板块：{link['board']}）→ {companies}")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", help="JSON source config path")
    parser.add_argument("--input-json", help="Analyze pre-collected news JSON instead of fetching")
    parser.add_argument("--chain-map", default=str(DEFAULT_CHAIN_MAP), help="Industry chain mapping txt path")
    parser.add_argument("--hours", type=int, default=24, help="Keep items from the last N hours when timestamps exist; <=0 disables")
    parser.add_argument("--limit", type=int, default=50, help="Maximum news items to process")
    parser.add_argument("--per-chain", type=int, default=5, help="Maximum companies per matched chain")
    parser.add_argument("--format", choices=["json", "markdown"], default="markdown")
    parser.add_argument("--matched-only", action="store_true", help="Only output news items that match at least one industry chain")
    parser.add_argument("--no-fetch-content", action="store_true", help="Do not fetch article detail pages for body-level analysis")
    args = parser.parse_args()

    if args.input_json:
        items = load_input_json(args.input_json)
    else:
        items = fetch_sources(load_sources(args.sources), args.limit)
    items = filter_recent(dedupe(items), args.hours)[: args.limit]
    if not args.no_fetch_content:
        items = [enrich_with_article_content(item) for item in items]
    chains = load_industry_chains(args.chain_map)
    results = [analyze_item(item, args.per_chain, chains) for item in items]
    if args.matched_only:
        results = [row for row in results if row["related_industries"]]

    if args.format == "json":
        print(json.dumps(results, ensure_ascii=False, indent=2))
    else:
        print(render_markdown(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
