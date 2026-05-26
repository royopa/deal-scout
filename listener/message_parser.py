from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlparse

URL_PATTERN = re.compile(
    r"(?:(?:https?://|www\.)[^\s<>\"]+|\b(?:[a-z0-9-]+\.)+[a-z]{2,}(?:/[^\s<>\"]*)?)",
    re.IGNORECASE,
)
PRICE_PATTERN = re.compile(
    r"(?<!\w)(R\$\s*)?(?P<value>(?:\d{1,3}(?:\.\d{3})+|\d{3,})(?:,\d{2})?|\d{1,2},\d{2})(?!\w)",
    re.IGNORECASE,
)
COUPON_PATTERNS = [
    re.compile(
        r"(?:cupom|c[oó]digo)\s*[:\-]?\s*([A-Z0-9][A-Z0-9_-]{2,})",
        re.IGNORECASE,
    ),
    re.compile(
        r"use\s+(?:o\s+)?(?:cupom|c[oó]digo)?\s*([A-Z0-9][A-Z0-9_-]{2,})",
        re.IGNORECASE,
    ),
    re.compile(
        r"com\s+o\s+cupom\s+([A-Z0-9][A-Z0-9_-]{2,})",
        re.IGNORECASE,
    ),
]

SHORTENER_DOMAINS = {
    "amzn.to",
    "bit.ly",
    "cutt.ly",
    "tinyurl.com",
    "t.co",
    "shope.ee",
    "s.click.aliexpress.com",
    "mercadolivre.com.br-secure-link",
}
MARKETPLACE_DOMAINS = {
    "amazon.com.br",
    "mercadolivre.com.br",
    "mercadolivre.com",
    "magazineluiza.com.br",
    "magalu.com",
    "americanas.com.br",
    "shopee.com.br",
    "aliexpress.com",
    "casasbahia.com.br",
    "pontofrio.com.br",
    "kabum.com.br",
    "carrefour.com.br",
    "extra.com.br",
    "fastshop.com.br",
}
AFFILIATE_HINTS = (
    "aff",
    "affiliate",
    "utm_",
    "ref=",
    "tag=",
    "promo=",
    "coupon=",
    "afiliado",
)
CURRENT_PRICE_HINTS = (
    "por",
    "agora",
    "sai",
    "saindo",
    "fica",
    "apenas",
    "oferta",
    "promo",
    "promoção",
)
PREVIOUS_PRICE_HINTS = (
    "de",
    "antes",
    "era",
    "preço normal",
    "preco normal",
    "custava",
)
DESCRIPTION_NOISE_PREFIXES = (
    "link",
    "compre",
    "clique",
    "acesse",
    "garanta",
    "aproveite",
    "corra",
)


def normalize_message_text(message_text: str | None) -> str:
    if not message_text:
        return ""

    text = (
        message_text.replace("\r\n", "\n")
        .replace("\r", "\n")
        .replace("\u00a0", " ")
        .replace("\u200b", "")
        .replace("\t", " ")
    )
    normalized_lines: list[str] = []
    previous_blank = False

    for raw_line in text.split("\n"):
        line = re.sub(r"\s+", " ", raw_line).strip()
        if not line:
            if not previous_blank:
                normalized_lines.append("")
            previous_blank = True
            continue
        normalized_lines.append(line)
        previous_blank = False

    return "\n".join(normalized_lines).strip()


def _sanitize_url(url: str) -> str:
    return url.rstrip(".,);!?]>\"'")


def extract_urls(message_text: str | None) -> list[str]:
    if not message_text:
        return []

    seen: set[str] = set()
    urls: list[str] = []
    for match in URL_PATTERN.finditer(message_text):
        url = _sanitize_url(match.group(0))
        if url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def message_contains_url(message_text: str | None) -> bool:
    return bool(extract_urls(message_text))


def _url_domain(url: str | None) -> str | None:
    if not url:
        return None
    normalized = url if re.match(r"^[a-z]+://", url, re.IGNORECASE) else f"https://{url}"
    parsed = urlparse(normalized)
    domain = parsed.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain or None


def _url_path_and_query(url: str | None) -> str:
    if not url:
        return ""
    normalized = url if re.match(r"^[a-z]+://", url, re.IGNORECASE) else f"https://{url}"
    parsed = urlparse(normalized)
    return f"{parsed.path}?{parsed.query}".lower()


def _is_shortener_domain(domain: str | None) -> bool:
    if not domain:
        return False
    return domain in SHORTENER_DOMAINS


def _is_marketplace_domain(domain: str | None) -> bool:
    if not domain:
        return False
    return domain in MARKETPLACE_DOMAINS or any(
        domain.endswith(f".{candidate}") for candidate in MARKETPLACE_DOMAINS
    )


def _is_affiliate_url(url: str | None) -> bool:
    if not url:
        return False
    domain = _url_domain(url)
    haystack = f"{domain or ''} {_url_path_and_query(url)}"
    return _is_shortener_domain(domain) or any(
        hint in haystack for hint in AFFILIATE_HINTS
    )


def _score_url(url: str, position: int, block_text: str) -> int:
    domain = _url_domain(url)
    score = max(0, 20 - position)

    if _is_marketplace_domain(domain):
        score += 20
    if not _is_shortener_domain(domain):
        score += 10
    if _is_affiliate_url(url):
        score -= 3
    if re.search(r"/(dp|gp/product|produto|product|offer|itm|p/)", _url_path_and_query(url)):
        score += 8
    if "oferta" in block_text.lower() or "promo" in block_text.lower():
        score += 2

    return score


def _select_primary_url(block_urls: list[str], block_text: str) -> str | None:
    if not block_urls:
        return None
    return max(
        block_urls,
        key=lambda candidate: _score_url(
            candidate,
            block_urls.index(candidate),
            block_text,
        ),
    )


def _parse_price_value(raw_value: str) -> Decimal | None:
    candidate = raw_value.strip().replace("R$", "").replace(" ", "")
    candidate = candidate.replace(".", "").replace(",", ".")
    try:
        return Decimal(candidate)
    except InvalidOperation:
        return None


def _format_decimal(value: Decimal | None) -> str | None:
    if value is None:
        return None
    return f"{value.quantize(Decimal('0.01'))}"


def _contains_hint(text: str, hints: tuple[str, ...]) -> bool:
    lowered = text.lower()
    for hint in hints:
        pattern = rf"(?<!\w){re.escape(hint)}(?!\w)"
        if re.search(pattern, lowered):
            return True
    return False


def _extract_price_candidates(text: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    lowered = text.lower()

    for match in PRICE_PATTERN.finditer(text):
        raw_text = match.group(0)
        raw_value = match.group("value")
        value = _parse_price_value(raw_value)
        if value is None:
            continue

        context_start = max(0, match.start() - 30)
        context_end = min(len(text), match.end() + 30)
        context = lowered[context_start:context_end]
        has_currency = "r$" in raw_text.lower() or "r$" in context
        has_hint = _contains_hint(context, CURRENT_PRICE_HINTS + PREVIOUS_PRICE_HINTS)
        if not has_currency and not has_hint and value < Decimal("100"):
            continue

        current_score = 1
        previous_score = 0

        if _contains_hint(context, CURRENT_PRICE_HINTS):
            current_score += 2
        if _contains_hint(context, PREVIOUS_PRICE_HINTS):
            previous_score += 3
        if re.search(rf"de\s+{re.escape(raw_text.lower())}", context):
            previous_score += 2
        if re.search(rf"por\s+{re.escape(raw_text.lower())}", context):
            current_score += 2

        candidates.append(
            {
                "text": raw_text.strip(),
                "value": value,
                "current_score": current_score,
                "previous_score": previous_score,
            }
        )

    return candidates


def _extract_prices(block_text: str) -> dict[str, str | None]:
    candidates = _extract_price_candidates(block_text)
    if not candidates:
        return {
            "product_price": None,
            "product_price_text": None,
            "product_original_price": None,
            "product_original_price_text": None,
            "price_currency": None,
        }

    current_candidate = max(
        candidates,
        key=lambda candidate: (
            candidate["current_score"],
            -candidate["value"],
        ),
    )
    current_value = current_candidate["value"]

    previous_candidates = [
        candidate
        for candidate in candidates
        if candidate is not current_candidate and candidate["value"] >= current_value
    ]
    previous_candidate = None
    if previous_candidates:
        previous_candidate = max(
            previous_candidates,
            key=lambda candidate: (
                candidate["previous_score"],
                candidate["value"],
            ),
        )
        if (
            previous_candidate["previous_score"] <= 0
            and previous_candidate["value"] == current_value
        ):
            previous_candidate = None

    return {
        "product_price": _format_decimal(current_value),
        "product_price_text": current_candidate["text"],
        "product_original_price": _format_decimal(
            previous_candidate["value"] if previous_candidate else None
        ),
        "product_original_price_text": (
            previous_candidate["text"] if previous_candidate else None
        ),
        "price_currency": "BRL",
    }


def _extract_coupon(block_text: str) -> tuple[str | None, str | None]:
    for pattern in COUPON_PATTERNS:
        match = pattern.search(block_text)
        if not match:
            continue
        coupon_code = match.group(1).strip().upper()
        for line in block_text.split("\n"):
            if coupon_code.lower() in line.lower():
                return coupon_code, line.strip()
        return coupon_code, match.group(0).strip()
    return None, None


def _clean_description_line(line: str) -> str:
    cleaned = URL_PATTERN.sub("", line)
    cleaned = re.sub(r"#\S+", "", cleaned)
    cleaned = re.sub(r"@\S+", "", cleaned)
    cleaned = re.sub(r"(?:cupom|c[oó]digo)\s*[:\-]?\s*[A-Z0-9_-]+", "", cleaned, flags=re.IGNORECASE)
    cleaned = PRICE_PATTERN.sub("", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned)
    cleaned = cleaned.strip(" -|•:;.,")
    return cleaned.strip()


def _extract_description(block_text: str) -> str | None:
    candidates: list[str] = []

    for line in block_text.split("\n"):
        lowered = line.lower()
        if not line.strip():
            continue
        if extract_urls(line) and len(line.split()) <= 3:
            continue
        if lowered.startswith(DESCRIPTION_NOISE_PREFIXES):
            continue
        cleaned = _clean_description_line(line)
        if not cleaned:
            continue
        if len(cleaned) <= 2:
            continue
        candidates.append(cleaned)

    if candidates:
        return " ".join(candidates[:2]).strip()

    fallback = _clean_description_line(block_text)
    return fallback or None


def _parse_status(product_url: str | None, product_price: str | None, product_description: str | None) -> str:
    required_hits = sum(
        bool(value)
        for value in (
            product_url,
            product_price,
            product_description,
        )
    )
    if required_hits == 3:
        return "complete"
    if required_hits:
        return "partial"
    return "empty"


def _looks_like_new_product_line(line: str) -> bool:
    lowered = line.lower()
    continuation_prefixes = (
        "cupom",
        "código",
        "codigo",
        "por",
        "de",
        "antes",
        "agora",
        "r$",
        "link",
    )
    return not _contains_hint(lowered[:20], continuation_prefixes)


def _split_product_blocks(normalized_text: str) -> list[str]:
    if not normalized_text:
        return [""]

    blocks: list[str] = []
    current_lines: list[str] = []
    current_has_url = False

    def flush() -> None:
        nonlocal current_lines, current_has_url
        if current_lines:
            blocks.append("\n".join(current_lines).strip())
        current_lines = []
        current_has_url = False

    for line in normalized_text.split("\n"):
        stripped = line.strip()
        if not stripped:
            flush()
            continue

        line_has_url = message_contains_url(stripped)
        if current_lines and current_has_url and line_has_url and _looks_like_new_product_line(stripped):
            flush()

        current_lines.append(stripped)
        current_has_url = current_has_url or line_has_url

    flush()
    return blocks or [normalized_text]


def _build_product(block_text: str, all_urls: list[str]) -> dict[str, Any]:
    block_urls = extract_urls(block_text)
    product_url = _select_primary_url(block_urls, block_text)
    coupon_code, coupon_text = _extract_coupon(block_text)
    price_data = _extract_prices(block_text)
    product_description = _extract_description(block_text)
    parse_status = _parse_status(
        product_url,
        price_data["product_price"],
        product_description,
    )
    parse_confidence = min(
        0.99,
        round(
            0.15
            + (0.30 if product_url else 0.0)
            + (0.30 if price_data["product_price"] else 0.0)
            + (0.20 if product_description else 0.0)
            + (0.05 if coupon_code else 0.0)
            + (0.05 if block_urls else 0.0),
            2,
        ),
    )

    return {
        "product_url": product_url,
        "product_domain": _url_domain(product_url),
        "product_description": product_description,
        "coupon_code": coupon_code,
        "coupon_text": coupon_text,
        "is_affiliate_url": _is_affiliate_url(product_url),
        "parse_status": parse_status,
        "parse_confidence": f"{parse_confidence:.2f}",
        "all_urls": all_urls,
        "url_count": len(all_urls),
        **price_data,
    }


def parse_deal_message(message_text: str | None) -> dict[str, Any]:
    normalized_text = normalize_message_text(message_text)
    all_urls = extract_urls(normalized_text)
    product_blocks = _split_product_blocks(normalized_text)
    products = [_build_product(block, all_urls) for block in product_blocks]

    products = sorted(
        products,
        key=lambda product: (
            product["parse_status"] != "complete",
            -float(product["parse_confidence"]),
        ),
    )

    return {
        "normalized_text": normalized_text,
        "all_urls": all_urls,
        "url_count": len(all_urls),
        "product_count": len(products),
        "products": products,
    }
