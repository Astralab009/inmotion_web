import asyncio
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

import asyncpg
import httpx
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

try:
    from openai import AsyncOpenAI
except Exception:  # OpenAI is optional. Keyword matching remains available.
    AsyncOpenAI = None


load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)
logger = logging.getLogger("in-motion")

DATABASE_URL = os.getenv("DATABASE_URL")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
FRONTEND_PATH = Path(__file__).with_name("index.html")

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/148.0.0.0 Safari/537.36"
)


@dataclass
class NewsItem:
    title: str
    summary: str
    content: str
    source: str
    url: str
    published_at: datetime


class AppState:
    pool: Optional[asyncpg.Pool] = None
    openai_client: Any = None


state = AppState()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def clean_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = re.sub(r"<script[\s\S]*?</script>", " ", text, flags=re.I)
    text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("&nbsp;", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def parse_time(value: Any) -> datetime:
    if not value:
        return utc_now()
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    text = str(value).strip()
    if text.isdigit():
        return parse_time(int(text))
    text = re.sub(r"年|/", "-", text)
    text = re.sub(r"月", "-", text)
    text = re.sub(r"日", " ", text)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return utc_now()


def absolutize_url(url: str, base: str) -> str:
    if not url:
        return base
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        match = re.match(r"https?://[^/]+", base)
        return f"{match.group(0)}{url}" if match else url
    return url


async def init_db_connection(conn: asyncpg.Connection) -> None:
    """Make asyncpg return json/jsonb fields as Python values."""
    for typename in ("json", "jsonb"):
        await conn.set_type_codec(
            typename,
            schema="pg_catalog",
            encoder=json.dumps,
            decoder=json.loads,
            format="text",
        )


def require_pool() -> asyncpg.Pool:
    if state.pool is None:
        raise HTTPException(status_code=503, detail="Database is not initialized. Please set DATABASE_URL.")
    return state.pool


def _row_to_dict(row: Optional[asyncpg.Record]) -> Optional[Dict[str, Any]]:
    return dict(row) if row is not None else None


async def _client_get_json(client: httpx.AsyncClient, url: str, **kwargs: Any) -> Any:
    response = await client.get(url, **kwargs)
    response.raise_for_status()
    return response.json()


async def _client_get_html(client: httpx.AsyncClient, url: str, **kwargs: Any) -> str:
    response = await client.get(url, **kwargs)
    response.raise_for_status()
    return response.text


def _make_item(
    *,
    title: Any,
    summary: Any,
    content: Any,
    source: str,
    url: str,
    published_at: Any,
) -> Optional[NewsItem]:
    clean_title = clean_text(title)[:180]
    clean_summary = clean_text(summary or content or title)[:300]
    clean_content = clean_text(content or summary or title)
    if not clean_title or not url:
        return None
    return NewsItem(
        title=clean_title,
        summary=clean_summary,
        content=clean_content,
        source=source,
        url=url,
        published_at=parse_time(published_at),
    )


async def fetch_wallstreetcn(client: Optional[httpx.AsyncClient] = None) -> List[NewsItem]:
    async def run(active_client: httpx.AsyncClient) -> List[NewsItem]:
        api_url = "https://api-one.wallstreetcn.com/apiv1/content/lives"
        try:
            payload = await _client_get_json(
                active_client,
                api_url,
                params={"channel": "global-channel", "limit": 30},
            )
            rows = (payload.get("data") or {}).get("items") or payload.get("items") or []
            items: List[NewsItem] = []
            for row in rows[:30]:
                live_id = row.get("id") or row.get("uri") or row.get("title")
                item = _make_item(
                    title=row.get("title") or row.get("content_text") or row.get("content"),
                    summary=row.get("content_short") or row.get("content_text") or row.get("content"),
                    content=row.get("content_text") or row.get("content") or row.get("title"),
                    source="华尔街见闻",
                    url=f"https://wallstreetcn.com/livenews/{live_id}",
                    published_at=row.get("display_time") or row.get("created_at") or row.get("updated_at"),
                )
                if item:
                    items.append(item)
            if items:
                return items
        except Exception as exc:
            logger.warning("WallstreetCN API failed, fallback to HTML: %s", exc)

        page_url = "https://wallstreetcn.com/lives/global"
        html = await _client_get_html(active_client, page_url)
        soup = BeautifulSoup(html, "html.parser")
        items = []
        for node in soup.select("a[href], article, div"):
            text = clean_text(node.get_text(" ", strip=True))
            href = node.get("href") if hasattr(node, "get") else None
            if len(text) < 8 or not href:
                continue
            if "livenews" not in href and "live" not in href:
                continue
            item = _make_item(
                title=text[:180],
                summary=text,
                content=text,
                source="华尔街见闻",
                url=absolutize_url(href, page_url),
                published_at=utc_now(),
            )
            if item:
                items.append(item)
            if len(items) >= 30:
                break
        return items

    if client:
        return await run(client)
    async with httpx.AsyncClient(headers=_request_headers(), follow_redirects=True, timeout=15) as own_client:
        return await run(own_client)


async def fetch_sina(client: Optional[httpx.AsyncClient] = None) -> List[NewsItem]:
    async def run(active_client: httpx.AsyncClient) -> List[NewsItem]:
        api_url = "https://feed.mix.sina.com.cn/api/roll/get"
        try:
            payload = await _client_get_json(
                active_client,
                api_url,
                params={"pageid": 153, "lid": 2509, "k": "", "num": 30, "page": 1},
            )
            rows = (payload.get("result") or {}).get("data") or payload.get("data") or []
            items: List[NewsItem] = []
            for row in rows[:30]:
                item = _make_item(
                    title=row.get("title"),
                    summary=row.get("intro") or row.get("summary") or row.get("title"),
                    content=row.get("intro") or row.get("summary") or row.get("title"),
                    source="新浪财经",
                    url=row.get("url") or row.get("wapurl") or "https://finance.sina.com.cn/",
                    published_at=row.get("ctime") or row.get("time"),
                )
                if item:
                    items.append(item)
            if items:
                return items
        except Exception as exc:
            logger.warning("Sina API failed, fallback to HTML: %s", exc)

        page_url = "https://finance.sina.com.cn/roll/index.d.html"
        html = await _client_get_html(active_client, page_url)
        soup = BeautifulSoup(html, "html.parser")
        items = []
        for link in soup.select("a[href]"):
            title = clean_text(link.get_text(" ", strip=True))
            href = link.get("href")
            if len(title) < 8 or not href:
                continue
            item = _make_item(
                title=title,
                summary=title,
                content=title,
                source="新浪财经",
                url=absolutize_url(href, page_url),
                published_at=utc_now(),
            )
            if item:
                items.append(item)
            if len(items) >= 30:
                break
        return items

    if client:
        return await run(client)
    async with httpx.AsyncClient(headers=_request_headers(), follow_redirects=True, timeout=15) as own_client:
        return await run(own_client)


async def fetch_eastmoney(client: Optional[httpx.AsyncClient] = None) -> List[NewsItem]:
    async def run(active_client: httpx.AsyncClient) -> List[NewsItem]:
        page_url = "https://kuaixun.eastmoney.com/"
        html = await _client_get_html(active_client, page_url)
        soup = BeautifulSoup(html, "html.parser")
        items: List[NewsItem] = []
        seen_titles: Set[str] = set()
        selectors = [
            "div.kuaixun-item a[href]",
            ".news_item a[href]",
            ".livenews-media a[href]",
            ".kuaixun-list a[href]",
            "a[href]",
        ]
        for selector in selectors:
            for link in soup.select(selector):
                title = clean_text(link.get_text(" ", strip=True))
                href = link.get("href")
                if len(title) < 8 or not href or title in seen_titles:
                    continue
                seen_titles.add(title)
                item = _make_item(
                    title=title,
                    summary=title,
                    content=title,
                    source="东方财富",
                    url=absolutize_url(href, page_url),
                    published_at=utc_now(),
                )
                if item:
                    items.append(item)
                if len(items) >= 30:
                    return items
        return items

    if client:
        return await run(client)
    async with httpx.AsyncClient(headers=_request_headers(), follow_redirects=True, timeout=15) as own_client:
        return await run(own_client)


async def fetch_cls(client: Optional[httpx.AsyncClient] = None) -> List[NewsItem]:
    async def run(active_client: httpx.AsyncClient) -> List[NewsItem]:
        url = "https://www.cls.cn/nodeapi/telegraphList"
        payload = await _client_get_json(
            active_client,
            url,
            params={"app": "Cailianpress", "os": "web", "refresh_type": 1, "sv": 8, "limit": 30},
            headers={"Referer": "https://www.cls.cn"},
        )
        data = payload.get("data") or {}
        rows = data.get("roll_data") or data.get("telegraph_list") or data.get("list") or []
        items: List[NewsItem] = []
        for row in rows[:30]:
            news_id = row.get("id") or row.get("article_id") or row.get("telegraph_id") or row.get("title")
            item = _make_item(
                title=row.get("title") or row.get("content") or row.get("brief"),
                summary=row.get("brief") or row.get("content") or row.get("title"),
                content=row.get("content") or row.get("brief") or row.get("title"),
                source="财联社",
                url=f"https://www.cls.cn/detail/{news_id}",
                published_at=row.get("mtime") or row.get("ctime") or row.get("time") or row.get("modified_time"),
            )
            if item:
                items.append(item)
        return items

    if client:
        return await run(client)
    async with httpx.AsyncClient(headers=_request_headers(), follow_redirects=True, timeout=15) as own_client:
        return await run(own_client)


def _request_headers() -> Dict[str, str]:
    return {
        "User-Agent": USER_AGENT,
        "Accept": "application/json,text/html;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    }


async def fetch_all_news() -> List[NewsItem]:
    logger.info("Starting news fetch from configured sources")
    async with httpx.AsyncClient(headers=_request_headers(), follow_redirects=True, timeout=15) as client:
        sources = [
            ("华尔街见闻", fetch_wallstreetcn(client)),
            ("新浪财经", fetch_sina(client)),
            ("东方财富", fetch_eastmoney(client)),
            ("财联社", fetch_cls(client)),
        ]
        results = await asyncio.gather(*(task for _, task in sources), return_exceptions=True)

    deduped: List[NewsItem] = []
    seen_urls: Set[str] = set()
    for (name, _), result in zip(sources, results):
        if isinstance(result, Exception):
            logger.warning("Fetch failed for %s: %s", name, result)
            continue
        for item in result:
            key = item.url.strip()
            if not key or key in seen_urls:
                continue
            seen_urls.add(key)
            deduped.append(item)
    logger.info("Fetched %s unique news items", len(deduped))
    return deduped


async def fetch_sector_knowledge(conn: asyncpg.Connection) -> List[Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT
            s.id AS sector_id,
            s.name AS sector_name,
            cs.segment_name,
            cs.position,
            c.name AS chain_name,
            COALESCE(
                json_agg(
                    DISTINCT jsonb_build_object(
                        'code', st.code,
                        'name', st.name,
                        'relevance', st.relevance::text
                    )
                ) FILTER (WHERE st.id IS NOT NULL),
                '[]'::json
            ) AS stocks
        FROM sectors s
        JOIN chain_segments cs ON cs.id = s.segment_id
        JOIN chains c ON c.id = cs.chain_id
        LEFT JOIN stocks st ON st.sector_id = s.id
        GROUP BY s.id, s.name, cs.segment_name, cs.position, c.name
        ORDER BY c.name, cs.position, cs.segment_name, s.name
        """
    )
    return [dict(row) for row in rows]


def build_keywords(sector: Dict[str, Any]) -> Set[str]:
    keywords = {
        str(sector.get("sector_name") or ""),
        str(sector.get("segment_name") or ""),
        str(sector.get("chain_name") or ""),
    }
    for stock in sector.get("stocks") or []:
        keywords.add(str(stock.get("name") or ""))
        keywords.add(str(stock.get("code") or ""))
    return {keyword.lower().strip() for keyword in keywords if len(keyword.strip()) >= 2}


async def _news_record(conn: asyncpg.Connection, news_record: Any) -> Dict[str, Any]:
    if isinstance(news_record, int):
        row = await conn.fetchrow("SELECT id, title, summary, content FROM news WHERE id = $1", news_record)
        if row is None:
            raise HTTPException(status_code=404, detail="News not found")
        return dict(row)
    if isinstance(news_record, asyncpg.Record):
        return dict(news_record)
    if isinstance(news_record, dict):
        return news_record
    raise TypeError("news_record must be a news id, asyncpg.Record, or dict")


def _news_text(news_record: Dict[str, Any]) -> str:
    return clean_text(
        f"{news_record.get('title') or ''} "
        f"{news_record.get('summary') or ''} "
        f"{news_record.get('content') or ''}"
    ).lower()


async def keyword_match(conn: asyncpg.Connection, news_record: Any) -> List[int]:
    news = await _news_record(conn, news_record)
    text = _news_text(news)
    matched: List[int] = []
    for sector in await fetch_sector_knowledge(conn):
        if any(keyword in text for keyword in build_keywords(sector)):
            matched.append(int(sector["sector_id"]))
    return list(dict.fromkeys(matched))


def _compact_knowledge(knowledge: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    compact: List[Dict[str, Any]] = []
    for item in knowledge:
        compact.append(
            {
                "sector_id": item.get("sector_id"),
                "sector_name": item.get("sector_name"),
                "segment_name": item.get("segment_name"),
                "position": item.get("position"),
                "chain_name": item.get("chain_name"),
                "stocks": [
                    {"code": st.get("code"), "name": st.get("name")}
                    for st in (item.get("stocks") or [])[:20]
                ],
            }
        )
    return compact


def _normalize_match_type(value: Any, default: str = "关键词匹配") -> str:
    text = clean_text(value)
    if text in {"精确匹配", "技术映射", "关键词匹配", "无匹配"}:
        return text
    if "技术" in text or "映射" in text:
        return "技术映射"
    if "精确" in text or "直接" in text:
        return "精确匹配"
    return default


async def gpt_match(conn: asyncpg.Connection, news_record: Any) -> Tuple[List[int], Optional[Dict[str, Any]]]:
    news = await _news_record(conn, news_record)
    if not OPENAI_API_KEY or AsyncOpenAI is None:
        return [], None

    if state.openai_client is None:
        state.openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

    knowledge = await fetch_sector_knowledge(conn)
    valid_ids = {int(item["sector_id"]) for item in knowledge}
    payload = {
        "instruction": (
            "请将新闻关联到最可能受影响的A股产业链板块。"
            "仅输出JSON对象，格式为："
            "{\"sector_ids\":[1,2],\"match_type\":\"精确匹配或技术映射\","
            "\"reason\":\"简要理由\",\"confidence\":0.0}"
        ),
        "sectors": _compact_knowledge(knowledge),
        "news": {
            "title": news.get("title"),
            "summary": news.get("summary"),
            "content": (news.get("content") or "")[:4000],
        },
    }
    try:
        response = await state.openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": "你是A股产业链新闻分析助手。必须输出严格JSON，不要输出Markdown。",
                },
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            temperature=0.1,
        )
        data = json.loads(response.choices[0].message.content or "{}")
        sector_ids = []
        for value in data.get("sector_ids") or []:
            try:
                sector_id = int(value)
            except (TypeError, ValueError):
                continue
            if sector_id in valid_ids:
                sector_ids.append(sector_id)
        data["match_type"] = _normalize_match_type(data.get("match_type"), default="精确匹配")
        return list(dict.fromkeys(sector_ids)), data
    except Exception as exc:
        logger.warning("OpenAI matching failed: %s", exc)
        return [], None


async def _table_columns(conn: asyncpg.Connection, table_name: str) -> Set[str]:
    rows = await conn.fetch(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = $1
        """,
        table_name,
    )
    return {str(row["column_name"]) for row in rows}


async def _insert_news_sector(
    conn: asyncpg.Connection,
    news_id: int,
    sector_id: int,
    detail: Optional[Dict[str, Any]],
) -> None:
    columns = await _table_columns(conn, "news_sectors")
    insert_columns = ["news_id", "sector_id"]
    values: List[Any] = [news_id, sector_id]
    if "match_type" in columns:
        insert_columns.append("match_type")
        values.append(_normalize_match_type((detail or {}).get("match_type"), default="关键词匹配"))
    if "analysis_detail" in columns:
        insert_columns.append("analysis_detail")
        values.append(detail or {})
    elif "analysis" in columns:
        insert_columns.append("analysis")
        values.append(detail or {})

    placeholders = ", ".join(f"${idx}" for idx in range(1, len(values) + 1))
    await conn.execute(
        f"""
        INSERT INTO news_sectors({", ".join(insert_columns)})
        VALUES ({placeholders})
        ON CONFLICT DO NOTHING
        """,
        *values,
    )


async def _ensure_news_associations_with_detail(
    conn: asyncpg.Connection,
    news_id: int,
) -> Tuple[List[int], Optional[Dict[str, Any]]]:
    existing = await conn.fetch("SELECT sector_id FROM news_sectors WHERE news_id = $1", news_id)
    if existing:
        return [int(row["sector_id"]) for row in existing], None

    news = await _news_record(conn, news_id)
    try:
        sector_ids, detail = await gpt_match(conn, news)
    except Exception as exc:
        logger.warning("GPT matching wrapper failed: %s", exc)
        sector_ids, detail = [], None

    if not sector_ids:
        sector_ids = await keyword_match(conn, news)
        detail = {"match_type": "关键词匹配", "reason": "关键词匹配命中"} if sector_ids else None

    for sector_id in sector_ids:
        await _insert_news_sector(conn, news_id, sector_id, detail)
    return sector_ids, detail


async def ensure_news_associations(conn: asyncpg.Connection, news_id: int) -> List[int]:
    sector_ids, _ = await _ensure_news_associations_with_detail(conn, news_id)
    return sector_ids


# Backward-compatible alias for the original skeleton.
ensure_news_sectors = ensure_news_associations


async def insert_news(conn: asyncpg.Connection, item: NewsItem) -> Optional[int]:
    existing = await conn.fetchrow("SELECT id FROM news WHERE url = $1", item.url)
    if existing:
        await ensure_news_associations(conn, int(existing["id"]))
        return None

    row = await conn.fetchrow(
        """
        INSERT INTO news(title, summary, content, source, url, published_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING id
        """,
        item.title,
        item.summary,
        item.content,
        item.source,
        item.url,
        item.published_at,
    )
    if row is None:
        return None
    news_id = int(row["id"])
    await ensure_news_associations(conn, news_id)
    logger.info("Inserted news id=%s source=%s title=%s", news_id, item.source, item.title)
    return news_id


async def crawl_news() -> Dict[str, int]:
    logger.info("Manual/news crawl started")
    news_items = await fetch_all_news()
    inserted = 0
    async with require_pool().acquire() as conn:
        for item in news_items:
            try:
                news_id = await insert_news(conn, item)
                if news_id is not None:
                    inserted += 1
            except Exception as exc:
                logger.warning("Failed to save news %s: %s", item.title, exc)
    logger.info("News crawl finished fetched=%s inserted=%s", len(news_items), inserted)
    return {"fetched": len(news_items), "inserted": inserted}


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not DATABASE_URL:
        logger.warning("DATABASE_URL is not set. API data endpoints will return 503.")
        yield
        return

    state.pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=int(os.getenv("DB_POOL_MIN_SIZE", "1")),
        max_size=int(os.getenv("DB_POOL_MAX_SIZE", "10")),
        init=init_db_connection,
    )
    logger.info("Database pool initialized")
    try:
        yield
    finally:
        if state.pool is not None:
            await state.pool.close()
            state.pool = None
            logger.info("Database pool closed")


app = FastAPI(title="In Motion API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in os.getenv("CORS_ORIGINS", "*").split(",")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def index():
    if FRONTEND_PATH.exists():
        return FileResponse(FRONTEND_PATH)
    return {"name": "In Motion API", "message": "index.html not found. API is running."}


@app.get("/api/health")
async def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "database": state.pool is not None,
        "openai": bool(OPENAI_API_KEY and AsyncOpenAI is not None),
        "time": utc_now().isoformat(),
    }


@app.post("/api/crawl")
async def api_crawl() -> Dict[str, int]:
    return await crawl_news()


@app.get("/api/news")
async def list_news(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
) -> List[Dict[str, Any]]:
    async with require_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                n.id, n.title, n.summary, n.content, n.source, n.url, n.published_at,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object('id', s.id, 'name', s.name))
                    FILTER (WHERE s.id IS NOT NULL),
                    '[]'::json
                ) AS sectors
            FROM news n
            LEFT JOIN news_sectors ns ON ns.news_id = n.id
            LEFT JOIN sectors s ON s.id = ns.sector_id
            GROUP BY n.id
            ORDER BY n.published_at DESC NULLS LAST, n.id DESC
            LIMIT $1 OFFSET $2
            """,
            limit,
            offset,
        )
    return [dict(row) for row in rows]


@app.get("/api/news/{news_id}")
async def get_news(news_id: int) -> Dict[str, Any]:
    async with require_pool().acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT
                n.id, n.title, n.summary, n.content, n.source, n.url, n.published_at,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object('id', s.id, 'name', s.name))
                    FILTER (WHERE s.id IS NOT NULL),
                    '[]'::json
                ) AS sectors
            FROM news n
            LEFT JOIN news_sectors ns ON ns.news_id = n.id
            LEFT JOIN sectors s ON s.id = ns.sector_id
            WHERE n.id = $1
            GROUP BY n.id
            """,
            news_id,
        )
    news = _row_to_dict(row)
    if news is None:
        raise HTTPException(status_code=404, detail="News not found")
    return news


@app.get("/api/analyze")
async def analyze_news(news_id: int = Query(..., ge=1)) -> Dict[str, Any]:
    async with require_pool().acquire() as conn:
        await _news_record(conn, news_id)
        _, llm_analysis = await _ensure_news_associations_with_detail(conn, news_id)
        columns = await _table_columns(conn, "news_sectors")
        match_type_expr = "ns.match_type" if "match_type" in columns else "'关键词匹配'"
        rows = await conn.fetch(
            f"""
            SELECT
                c.id AS chain_id,
                c.name AS chain_name,
                cs.id AS segment_id,
                cs.segment_name,
                cs.position,
                s.id AS sector_id,
                s.name AS sector_name,
                {match_type_expr} AS match_type,
                COALESCE(
                    json_agg(
                        DISTINCT jsonb_build_object(
                            'code', st.code,
                            'name', st.name,
                            'relevance', st.relevance::text
                        )
                    ) FILTER (WHERE st.id IS NOT NULL),
                    '[]'::json
                ) AS stocks
            FROM news_sectors ns
            JOIN sectors s ON s.id = ns.sector_id
            JOIN chain_segments cs ON cs.id = s.segment_id
            JOIN chains c ON c.id = cs.chain_id
            LEFT JOIN stocks st ON st.sector_id = s.id
            WHERE ns.news_id = $1
            GROUP BY c.id, c.name, cs.id, cs.segment_name, cs.position, s.id, s.name, match_type
            ORDER BY c.name, cs.position, cs.segment_name, s.name
            """,
            news_id,
        )

    chains: Dict[int, Dict[str, Any]] = {}
    match_types: Set[str] = set()
    for row in rows:
        chain_id = int(row["chain_id"])
        chain = chains.setdefault(
            chain_id,
            {
                "chain_name": row["chain_name"],
                "segments": [],
                "_segment_map": {},
            },
        )
        match_types.add(row["match_type"] or "关键词匹配")
        segment_key = (row["position"], row["segment_name"])
        segment = chain["_segment_map"].setdefault(
            segment_key,
            {"position": row["position"], "segment_name": row["segment_name"], "sectors": []},
        )
        segment["sectors"].append(
            {"sector_id": row["sector_id"], "sector_name": row["sector_name"], "stocks": row["stocks"] or []}
        )

    affected_chains: List[Dict[str, Any]] = []
    position_order = {"upstream": 0, "midstream": 1, "downstream": 2}
    for chain in chains.values():
        segments = list(chain.pop("_segment_map").values())
        segments.sort(key=lambda item: position_order.get(item["position"], 9))
        chain["segments"] = segments
        affected_chains.append(chain)

    if not affected_chains:
        result_match_type = "无匹配"
    elif llm_analysis and llm_analysis.get("match_type"):
        result_match_type = _normalize_match_type(llm_analysis["match_type"], default="精确匹配")
    else:
        result_match_type = "技术映射" if "技术映射" in match_types else "关键词匹配"

    return {
        "news_id": news_id,
        "match_type": result_match_type,
        "affected_chains": affected_chains,
    }


@app.get("/api/sectors")
async def list_sectors() -> List[Dict[str, Any]]:
    async with require_pool().acquire() as conn:
        return await fetch_sector_knowledge(conn)


@app.get("/api/sectors/trending")
async def trending_sectors(limit: int = Query(20, ge=1, le=100)) -> List[Dict[str, Any]]:
    async with require_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                s.id,
                s.name,
                c.name AS chain_name,
                COUNT(DISTINCT n.id)::int AS news_count
            FROM sectors s
            JOIN chain_segments cs ON cs.id = s.segment_id
            JOIN chains c ON c.id = cs.chain_id
            JOIN news_sectors ns ON ns.sector_id = s.id
            JOIN news n ON n.id = ns.news_id
            WHERE n.published_at >= NOW() - INTERVAL '24 hours'
            GROUP BY s.id, s.name, c.name
            ORDER BY news_count DESC, s.name
            LIMIT $1
            """,
            limit,
        )
    return [dict(row) for row in rows]


@app.get("/api/sectors/{sector_id}")
async def get_sector(sector_id: int) -> Dict[str, Any]:
    async with require_pool().acquire() as conn:
        sector = await conn.fetchrow(
            """
            SELECT
                s.id, s.name, cs.segment_name, cs.position,
                c.id AS chain_id, c.name AS chain_name, NULL::text AS chain_category
            FROM sectors s
            JOIN chain_segments cs ON cs.id = s.segment_id
            JOIN chains c ON c.id = cs.chain_id
            WHERE s.id = $1
            """,
            sector_id,
        )
        if sector is None:
            raise HTTPException(status_code=404, detail="Sector not found")
        stocks = await conn.fetch(
            """
            SELECT code, name, relevance::text AS relevance
            FROM stocks
            WHERE sector_id = $1
            ORDER BY relevance DESC NULLS LAST, name
            """,
            sector_id,
        )
        news_rows = await conn.fetch(
            """
            SELECT
                n.id, n.title, n.summary, n.content, n.source, n.url, n.published_at,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object('id', s2.id, 'name', s2.name))
                    FILTER (WHERE s2.id IS NOT NULL),
                    '[]'::json
                ) AS sectors
            FROM news n
            JOIN news_sectors ns ON ns.news_id = n.id
            LEFT JOIN news_sectors ns2 ON ns2.news_id = n.id
            LEFT JOIN sectors s2 ON s2.id = ns2.sector_id
            WHERE ns.sector_id = $1
            GROUP BY n.id
            ORDER BY n.published_at DESC NULLS LAST, n.id DESC
            LIMIT 50
            """,
            sector_id,
        )
    return {
        "sector": dict(sector),
        "stocks": [dict(row) for row in stocks],
        "news": [dict(row) for row in news_rows],
    }


@app.get("/api/stocks/{stock_code}")
async def get_stock(stock_code: str) -> Dict[str, Any]:
    async with require_pool().acquire() as conn:
        stock = await conn.fetchrow(
            """
            SELECT
                st.code, st.name, st.relevance::text AS relevance,
                s.id AS sector_id, s.name AS sector_name,
                cs.segment_name, cs.position,
                c.id AS chain_id, c.name AS chain_name, NULL::text AS chain_category
            FROM stocks st
            JOIN sectors s ON s.id = st.sector_id
            JOIN chain_segments cs ON cs.id = s.segment_id
            JOIN chains c ON c.id = cs.chain_id
            WHERE lower(st.code) = lower($1)
            """,
            stock_code,
        )
        if stock is None:
            raise HTTPException(status_code=404, detail="Stock not found")
        news_rows = await conn.fetch(
            """
            SELECT
                n.id, n.title, n.summary, n.content, n.source, n.url, n.published_at,
                COALESCE(
                    json_agg(DISTINCT jsonb_build_object('id', s2.id, 'name', s2.name))
                    FILTER (WHERE s2.id IS NOT NULL),
                    '[]'::json
                ) AS sectors
            FROM news n
            JOIN news_sectors ns ON ns.news_id = n.id
            JOIN stocks st ON st.sector_id = ns.sector_id
            LEFT JOIN news_sectors ns2 ON ns2.news_id = n.id
            LEFT JOIN sectors s2 ON s2.id = ns2.sector_id
            WHERE lower(st.code) = lower($1)
            GROUP BY n.id
            ORDER BY n.published_at DESC NULLS LAST, n.id DESC
            LIMIT 80
            """,
            stock_code,
        )
    stock_detail = dict(stock)
    stock_detail["company_intro"] = "公司介绍暂未接入，后续可对接公告、年报或行情数据源补充。"
    return {"stock": stock_detail, "news": [dict(row) for row in news_rows]}


@app.get("/api/search")
async def search(q: str = Query(..., min_length=1), limit: int = Query(20, ge=1, le=50)) -> Dict[str, Any]:
    keyword = f"%{q.strip()}%"
    async with require_pool().acquire() as conn:
        sectors = await conn.fetch(
            """
            SELECT id, name
            FROM sectors
            WHERE name ILIKE $1
            ORDER BY name
            LIMIT $2
            """,
            keyword,
            limit,
        )
        stocks = await conn.fetch(
            """
            SELECT code, name, relevance::text AS relevance
            FROM stocks
            WHERE name ILIKE $1 OR code ILIKE $1
            ORDER BY name
            LIMIT $2
            """,
            keyword,
            limit,
        )
        news = await conn.fetch(
            """
            SELECT id, title, summary, source, url, published_at
            FROM news
            WHERE title ILIKE $1 OR summary ILIKE $1 OR content ILIKE $1
            ORDER BY published_at DESC NULLS LAST, id DESC
            LIMIT $2
            """,
            keyword,
            limit,
        )
    return {
        "q": q,
        "sectors": [dict(row) for row in sectors],
        "stocks": [dict(row) for row in stocks],
        "news": [dict(row) for row in news],
    }


@app.get("/api/chains")
async def list_chains() -> List[Dict[str, Any]]:
    async with require_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                c.id AS chain_id,
                c.name AS chain_name,
                NULL::text AS category,
                cs.id AS segment_id,
                cs.segment_name,
                cs.position,
                COALESCE(
                    json_agg(
                        DISTINCT jsonb_build_object('id', s.id, 'name', s.name)
                    ) FILTER (WHERE s.id IS NOT NULL),
                    '[]'::json
                ) AS sectors
            FROM chains c
            LEFT JOIN chain_segments cs ON cs.chain_id = c.id
            LEFT JOIN sectors s ON s.segment_id = cs.id
            GROUP BY c.id, c.name, cs.id, cs.segment_name, cs.position
            ORDER BY c.name, cs.position, cs.segment_name
            """
        )
    chains: Dict[int, Dict[str, Any]] = {}
    for row in rows:
        chain_id = int(row["chain_id"])
        chain = chains.setdefault(
            chain_id,
            {"id": chain_id, "name": row["chain_name"], "category": row["category"], "segments": []},
        )
        if row["segment_id"] is not None:
            chain["segments"].append(
                {
                    "id": row["segment_id"],
                    "name": row["segment_name"],
                    "segment_name": row["segment_name"],
                    "position": row["position"],
                    "sectors": row["sectors"] or [],
                }
            )
    return list(chains.values())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api_server:app", host="0.0.0.0", port=8000, reload=True)
