#!/usr/bin/env python3
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, Tuple
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import requests
from bs4 import BeautifulSoup

def get_jst_zone() -> ZoneInfo:
    try:
        return ZoneInfo("Asia/Tokyo")
    except ZoneInfoNotFoundError as exc:
        raise RuntimeError(
            "缺少时区数据库。请先执行: pip install tzdata"
        ) from exc


JST = get_jst_zone()
NOW_JST = datetime.now(JST)
KEYWORDS = ("ミート＆グリート", "抽選購入申込", "応募受付", "シングル")
SCHEDULE_MARKERS = ("抽選購入申込スケジュール", "抽選購入申込", "応募受付")

# 乃木坂新闻列表为前端渲染，静态 HTML 无详情链接；官方页面通过该 API 拉取列表与正文摘要。
NOGI_NEWS_API = "https://www.nogizaka46.com/s/n46/api/list/news_v2"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8,zh-CN;q=0.7",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}

GROUPS = [
    {"group": "乃木坂46", "base": "https://www.nogizaka46.com", "news": "https://www.nogizaka46.com/s/n46/news/list"},
    {"group": "櫻坂46", "base": "https://sakurazaka46.com", "news": "https://sakurazaka46.com/s/s46/news/list"},
    {"group": "日向坂46", "base": "https://www.hinatazaka46.com", "news": "https://www.hinatazaka46.com/s/official/news/list"},
]

FORTUNE_GROUPS = [
    {"group": "乃木坂46", "url": "https://ticket.fortunemeets.app/nogizaka46"},
    {"group": "櫻坂46", "url": "https://ticket.fortunemeets.app/sakurazaka46"},
    {"group": "日向坂46", "url": "https://ticket.fortunemeets.app/hinatazaka46"},
]


@dataclass
class NewsCandidate:
    group: str
    title: str
    url: str


def fetch_with_retry(url: str, retries: int = 3, timeout: int = 15) -> str:
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            response.encoding = response.apparent_encoding or "utf-8"
            return response.text
        except requests.RequestException:
            if attempt == retries:
                raise
            time.sleep(1.5 * attempt)
    raise RuntimeError(f"failed to fetch {url}")


def fetch_json_with_retry(url: str, retries: int = 3, timeout: int = 15):
    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError):
            if attempt == retries:
                raise
            time.sleep(1.5 * attempt)
    raise RuntimeError(f"failed to fetch json {url}")


def normalize_space(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def build_dt(year: int, month: int, day: int, hour: int, minute: int) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=JST)


def parse_schedule_windows(detail_text: str) -> List[Tuple[str, datetime, datetime]]:
    """
    从类似:
    【第4次応募受付】 2026年4月30日（木）14：00 ～ 5月1日（金）14：00 まで
    的文本中提取多轮申请时间区间。
    """
    compact = normalize_space(detail_text)
    pattern = re.compile(
        r"【(?P<round>第\d+次応募受付)】\s*"
        r"(?:(?P<sy>\d{4})年)?\s*(?P<sm>\d{1,2})月\s*(?P<sd>\d{1,2})日"
        r"(?:[（(][^)）]*[）)])?\s*(?P<sh>\d{1,2})[:：]\s*(?P<smin>\d{1,2})\s*"
        r"[～~]\s*"
        r"(?:(?P<ey>\d{4})年)?\s*(?P<em>\d{1,2})月\s*(?P<ed>\d{1,2})日"
        r"(?:[（(][^)）]*[）)])?\s*(?P<eh>\d{1,2})[:：]\s*(?P<emin>\d{1,2})"
    )
    windows: List[Tuple[str, datetime, datetime]] = []
    for m in pattern.finditer(compact):
        sy = int(m.group("sy")) if m.group("sy") else NOW_JST.year
        sm = int(m.group("sm"))
        sd = int(m.group("sd"))
        sh = int(m.group("sh"))
        smin = int(m.group("smin"))
        start = build_dt(sy, sm, sd, sh, smin)

        ey = int(m.group("ey")) if m.group("ey") else sy
        em = int(m.group("em"))
        ed = int(m.group("ed"))
        eh = int(m.group("eh"))
        emin = int(m.group("emin"))
        end = build_dt(ey, em, ed, eh, emin)
        if end < start:
            end = build_dt(ey + 1, em, ed, eh, emin)

        windows.append((m.group("round"), start, end))
    return windows


def to_halfwidth(text: str) -> str:
    trans = str.maketrans("０１２３４５６７８９：～　", "0123456789:~ ")
    return text.translate(trans)


def parse_jp_datetime(part: str, default_year: int) -> datetime:
    s = to_halfwidth(part)
    m = re.search(
        r"(?:(?P<year>\d{4})年)?\s*(?P<month>\d{1,2})月\s*(?P<day>\d{1,2})日"
        r"(?:[（(][^)）]*[）)])?\s*(?P<hour>\d{1,2})[:：]\s*(?P<minute>\d{1,2})",
        s,
    )
    if not m:
        raise ValueError(f"invalid datetime: {part}")
    year = int(m.group("year")) if m.group("year") else default_year
    return build_dt(year, int(m.group("month")), int(m.group("day")), int(m.group("hour")), int(m.group("minute")))


def parse_fortunemeets_windows(detail_text: str) -> List[Tuple[str, datetime, datetime]]:
    compact = to_halfwidth(normalize_space(detail_text))
    pattern = re.compile(
        r"(?P<label>応募期間|第\d+次応募(?:者)?(?:保障期間)?|保障期間)\s*"
        r"(?P<start>(?:\d{4}年)?\d{1,2}月\d{1,2}日(?:[（(][^)）]*[）)])?\d{1,2}:\d{2})\s*[~～]\s*"
        r"(?P<end>(?:\d{4}年)?\d{1,2}月\d{1,2}日(?:[（(][^)）]*[）)])?\d{1,2}:\d{2})"
    )
    out: List[Tuple[str, datetime, datetime]] = []
    for m in pattern.finditer(compact):
        label = m.group("label")
        start = parse_jp_datetime(m.group("start"), NOW_JST.year)
        end = parse_jp_datetime(m.group("end"), start.year)
        if end < start:
            end = build_dt(end.year + 1, end.month, end.day, end.hour, end.minute)
        out.append((label, start, end))
    return out


def parse_fortunemeets_links(root_url: str, list_html: str) -> List[str]:
    soup = BeautifulSoup(list_html, "html.parser")
    base = urlparse(root_url)
    links = set()
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if not href:
            continue
        full = urljoin(root_url, href)
        p = urlparse(full)
        if p.netloc != base.netloc:
            continue
        if not p.path.startswith(base.path.rstrip("/") + "/"):
            continue
        if full.rstrip("/") == root_url.rstrip("/"):
            continue
        links.add(full)
    return sorted(links)


def extract_fortunemeets_title(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for sel in ("h1", "h2", "title"):
        el = soup.select_one(sel)
        if el and normalize_space(el.get_text(" ", strip=True)):
            return normalize_space(el.get_text(" ", strip=True))
    return "forTUNE meets 活动"


def scrape_fortunemeets_group(group: str, root_url: str) -> List[dict]:
    try:
        root_html = fetch_with_retry(root_url)
    except requests.RequestException:
        return []
    detail_urls = parse_fortunemeets_links(root_url, root_html)
    rows: List[dict] = []
    for url in detail_urls:
        try:
            detail_html = fetch_with_retry(url)
        except requests.RequestException:
            continue
        text = BeautifulSoup(detail_html, "html.parser").get_text("\n", strip=True)
        windows = parse_fortunemeets_windows(text)
        if not windows:
            continue
        title = extract_fortunemeets_title(detail_html)
        for round_name, apply_start, apply_end in windows:
            if apply_end <= NOW_JST:
                continue
            status = "未开始" if apply_start > NOW_JST else "进行中"
            rows.append(
                {
                    "group": group,
                    "title": title,
                    "round": round_name,
                    "applyStart": apply_start.isoformat(),
                    "applyEnd": apply_end.isoformat(),
                    "status": status,
                    "url": url,
                    "source": "fortunemeets",
                }
            )
    return rows


def title_matches_keywords(title: str) -> bool:
    """列表标题里可能出现半角 &，关键词使用全角 ＆，统一后再匹配。"""
    normalized = (
        title.replace("&", "＆")
        .replace("∧", "＆")
        .replace("﹠", "＆")
    )
    return any(kw in normalized for kw in KEYWORDS)


def parse_news_list(group: str, base_url: str, list_html: str) -> List[NewsCandidate]:
    soup = BeautifulSoup(list_html, "html.parser")
    found = {}
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        title = normalize_space(a.get_text(" ", strip=True))
        if not href or not title:
            continue
        if not title_matches_keywords(title):
            continue
        if href.startswith("javascript:"):
            continue
        full_url = urljoin(base_url, href)
        found[full_url] = NewsCandidate(group=group, title=title, url=full_url)
    return list(found.values())


def scrape_group(group: str, base_url: str, news_url: str) -> List[dict]:
    rows: List[dict] = []

    if group == "乃木坂46":
        try:
            payload = fetch_json_with_retry(f"{NOGI_NEWS_API}?rw=400")
        except (requests.RequestException, ValueError):
            return rows
        seen_urls = set()
        for item in payload.get("data") or []:
            title = normalize_space(item.get("title") or "")
            link_url = (item.get("link_url") or "").strip()
            if not title or not link_url:
                continue
            if not title_matches_keywords(title):
                continue
            if link_url in seen_urls:
                continue
            seen_urls.add(link_url)

            text = item.get("text") or ""
            if not text or not any(marker in text for marker in SCHEDULE_MARKERS):
                try:
                    detail_html = fetch_with_retry(link_url)
                except requests.RequestException:
                    continue
                text = BeautifulSoup(detail_html, "html.parser").get_text("\n", strip=True)
            if not any(marker in text for marker in SCHEDULE_MARKERS):
                continue
            schedule_windows = parse_schedule_windows(text)
            for round_name, apply_start, apply_end in schedule_windows:
                if apply_end <= NOW_JST:
                    continue
                status = "未开始" if apply_start > NOW_JST else "进行中"
                rows.append(
                    {
                        "group": group,
                        "title": title,
                        "round": round_name,
                        "applyStart": apply_start.isoformat(),
                        "applyEnd": apply_end.isoformat(),
                        "status": status,
                        "url": link_url,
                        "source": "official_news",
                    }
                )
        return rows

    list_html = fetch_with_retry(news_url)
    candidates = parse_news_list(group, base_url, list_html)
    for candidate in candidates:
        try:
            detail_html = fetch_with_retry(candidate.url)
        except requests.RequestException:
            continue
        text = BeautifulSoup(detail_html, "html.parser").get_text("\n", strip=True)
        if not any(marker in text for marker in SCHEDULE_MARKERS):
            continue
        schedule_windows = parse_schedule_windows(text)
        for round_name, apply_start, apply_end in schedule_windows:
            if apply_end <= NOW_JST:
                continue
            status = "未开始" if apply_start > NOW_JST else "进行中"
            rows.append(
                {
                    "group": candidate.group,
                    "title": candidate.title,
                    "round": round_name,
                    "applyStart": apply_start.isoformat(),
                    "applyEnd": apply_end.isoformat(),
                    "status": status,
                    "url": candidate.url,
                    "source": "official_news",
                }
            )
    return rows


def main() -> None:
    all_rows: List[dict] = []
    for g in GROUPS:
        try:
            all_rows.extend(scrape_group(g["group"], g["base"], g["news"]))
        except requests.RequestException:
            continue

    for fg in FORTUNE_GROUPS:
        all_rows.extend(scrape_fortunemeets_group(fg["group"], fg["url"]))

    dedup = {}
    for row in all_rows:
        key = (row["group"], row["title"], row.get("round"), row["applyStart"], row["applyEnd"], row["url"])
        dedup[key] = row
    all_rows = list(dedup.values())

    all_rows.sort(key=lambda x: (x["applyStart"] or x["applyEnd"], x["applyEnd"]))
    output = {
        "generatedAt": NOW_JST.isoformat(),
        "timezone": "Asia/Tokyo",
        "items": all_rows,
    }
    out_path = Path(__file__).with_name("data.json")
    out_path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"saved {len(all_rows)} items -> {out_path}")


if __name__ == "__main__":
    main()
