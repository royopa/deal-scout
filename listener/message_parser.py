import re
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urlparse


URL_PATTERN = re.compile(
    r"(https?://\S+|www\.\S+|\b\S+\.(?:com|net|org|io|co|deals|com\.br)\b)",
    re.IGNORECASE,
)
PRICE_PATTERN = re.compile(
    r"(R\$\s*\d{1,3}(?:\.\d{3})*(?:,\d{2})?|R\$\s*\d+(?:,\d{2})?|\d{1,3}(?:\.\d{3})+(?:,\d{2})?|\d+,\d{2}|\d{3,6})"
)
COUPON_PATTERNS = [
    re.compile(
        r"(?:cupom|c[oó]digo)\s*(?:de\s*desconto\s*)?[:\-]?\s*([A-Z0-9][A-Z0-9_-]{2,24})",
        re.IGNORECASE,
    ),
    re.compile(
        r"use\s+(?:o\s+)?cupom\s*[:\-]?\s*([A-Z0-9][A-Z0-9_-]{2,24})",
        re.IGNORECASE,
    ),
    re.compile(r"use\s+([A-Z0-9][A-Z0-9_-]{2,24})", re.IGNORECASE),
]
PRODUCT_CONTEXT_PATTERN = re.compile(
    r"(produto|oferta|promo|promoção|desconto|aproveite|compre|link|loja)",
    re.IGNORECASE,
)
IGNORED_DESCRIPTION_LINE_PATTERN = re.compile(
    r"^(https?://|www\.|#|cupom|c[oó]digo|use\s+|clique|acesse|link\b)",
    re.IGNORECASE,
)

AFFILIATE_HINTS = (
    "utm_",
    "aff",
    "affiliate",
    "ref=",
    "ref_",
    "campaign",
    "tag=",
    "sck=",
    "ranmid",
    "ran_eid",
    "subid",
)
SHORTENER_DOMAINS = {
    "bit.ly",
    "tinyurl.com",
    "t.co",
    "amzn.to",
    "mercadolivre.com.br",
}
MARKETPLACE_HINTS = (
    "amazon.",
    "mercadolivre",
    "magazineluiza",
    "americanas",
    "shopee",
    "aliexpress",
    "kabum",
    "casasbahia",
    "ponto",
    "carrefour",
)
URL_CONTEXT_WINDOW = 80
PRICE_CONTEXT_WINDOW = 40
MAX_PRICE_BRL = Decimal("10000000")
MAX_PRICE_LINE_LENGTH = 18
MIN_DESCRIPTION_LENGTH = 4
MAX_DESCRIPTION_LENGTH = 280
CONFIDENCE_BASE = 0.1
CONFIDENCE_URL_WEIGHT = 0.35
CONFIDENCE_PRICE_WEIGHT = 0.25
CONFIDENCE_DESCRIPTION_WEIGHT = 0.2
CONFIDENCE_COUPON_WEIGHT = 0.1
CONFIDENCE_ORIGINAL_PRICE_WEIGHT = 0.1
DESCRIPTION_STRIP_CHARS = " -•\t"


def normalize_message_text(message: str | None) -> str:
    text = (message or "").replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\u200b", "").replace("\ufeff", "")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _strip_url_trailing_punctuation(url: str) -> str:
    return url.rstrip(".,;:!?)]}>\"'")


def extract_urls(message: str | None) -> list[str]:
    if not message:
        return []
    found = [
        _strip_url_trailing_punctuation(url) for url in URL_PATTERN.findall(message)
    ]
    deduped: list[str] = []
    seen: set[str] = set()
    for url in found:
        key = url.lower()
        if key in seen:
            continue
        deduped.append(url)
        seen.add(key)
    return deduped


def message_contains_url(message: str | None) -> bool:
    if not message:
        return False
    return bool(URL_PATTERN.search(message))


def _to_url_candidate(url: str) -> str:
    if url.lower().startswith(("http://", "https://")):
        return url
    return f"https://{url}"


def extract_domain(url: str | None) -> str | None:
    if not url:
        return None
    parsed = urlparse(_to_url_candidate(url))
    domain = parsed.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain or None


def is_affiliate_url(url: str | None) -> bool:
    if not url:
        return False
    lowered = _to_url_candidate(url).lower()
    parsed = urlparse(lowered)
    domain = parsed.netloc
    query = parsed.query
    return (
        any(hint in lowered for hint in AFFILIATE_HINTS)
        or domain in SHORTENER_DOMAINS
        or any(hint in query for hint in AFFILIATE_HINTS)
    )


def _score_url(url: str, message: str) -> int:
    score = 0
    lower_url = url.lower()
    domain = extract_domain(url) or ""
    if lower_url.startswith(("https://", "http://")):
        score += 10
    if domain and domain not in SHORTENER_DOMAINS:
        score += 10
    if any(hint in domain for hint in MARKETPLACE_HINTS):
        score += 25
    if is_affiliate_url(url):
        score += 5
    match = re.search(re.escape(url), message, re.IGNORECASE)
    if match:
        left = max(0, match.start() - URL_CONTEXT_WINDOW)
        right = min(len(message), match.end() + URL_CONTEXT_WINDOW)
        context = message[left:right]
        if PRODUCT_CONTEXT_PATTERN.search(context):
            score += 15
    return score


def _normalize_brl_price(raw: str) -> float | None:
    candidate = raw.upper().replace("R$", "").strip()
    candidate = candidate.replace(" ", "")
    if "," in candidate:
        candidate = candidate.replace(".", "").replace(",", ".")
    elif candidate.count(".") > 1:
        candidate = candidate.replace(".", "")
    elif "." in candidate:
        integer, _, fraction = candidate.partition(".")
        if len(fraction) == 3:
            candidate = integer + fraction
    if not re.fullmatch(r"\d+(\.\d{1,2})?", candidate):
        return None
    try:
        value = Decimal(candidate)
    except InvalidOperation:
        return None
    if value <= 0 or value > MAX_PRICE_BRL:
        return None
    return float(value)


def _extract_price_candidates(message: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for match in PRICE_PATTERN.finditer(message):
        raw = match.group(1).strip()
        value = _normalize_brl_price(raw)
        if value is None:
            continue
        start = match.start()
        left = max(0, start - PRICE_CONTEXT_WINDOW)
        context = message[left : match.end()].lower()
        candidates.append(
            {
                "raw": raw,
                "value": value,
                "start": start,
                "context": context,
            }
        )
    return candidates


def _select_current_and_original_price(
    candidates: list[dict[str, Any]],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None, bool]:
    if not candidates:
        return None, None, False

    current = None
    previous = None
    used_fallback = False
    current_keywords = ("por", "sai por", "agora", "final", "fica", "pague")
    previous_keywords = ("de ", "antes", "era", "preço original", "normalmente")

    for candidate in candidates:
        if previous is None and any(
            keyword in candidate["context"] for keyword in previous_keywords
        ):
            previous = candidate
            continue
        if current is None and any(
            keyword in candidate["context"] for keyword in current_keywords
        ):
            current = candidate

    if current is None:
        current = min(candidates, key=lambda item: item["value"])
        used_fallback = True
    if previous is None and len(candidates) > 1:
        previous = max(candidates, key=lambda item: item["value"])
        if previous["value"] <= current["value"]:
            previous = None

    return current, previous, used_fallback


def _extract_coupon_data(message: str) -> tuple[str | None, str | None]:
    code = None
    for pattern in COUPON_PATTERNS:
        match = pattern.search(message)
        if match:
            code = match.group(1).upper()
            break
    if not code:
        return None, None

    lines = [line.strip() for line in message.splitlines() if line.strip()]
    coupon_line = next(
        (line for line in lines if re.search(r"cupom|c[oó]digo|use", line, re.I)),
        None,
    )
    return code, coupon_line


def _extract_description(message: str, urls: list[str]) -> str | None:
    lines = [
        line.strip(DESCRIPTION_STRIP_CHARS)
        for line in message.splitlines()
        if line.strip()
    ]
    if not lines:
        return None

    url_set = {url.lower() for url in urls}

    for line in lines:
        lowered = line.lower()
        if IGNORED_DESCRIPTION_LINE_PATTERN.search(lowered):
            continue
        if any(url in lowered for url in url_set):
            continue
        if PRICE_PATTERN.search(line) and len(line) <= MAX_PRICE_LINE_LENGTH:
            continue
        if len(line) < MIN_DESCRIPTION_LENGTH:
            continue
        return line

    fallback = lines[0]
    return fallback[:MAX_DESCRIPTION_LENGTH] if fallback else None


def parse_structured_message(message: str | None) -> dict[str, Any]:
    normalized_message = normalize_message_text(message)
    urls = extract_urls(normalized_message)

    scored_urls = sorted(
        ((url, _score_url(url, normalized_message)) for url in urls),
        key=lambda item: item[1],
        reverse=True,
    )
    product_url = scored_urls[0][0] if scored_urls else None
    product_domain = extract_domain(product_url)
    affiliate_flag = is_affiliate_url(product_url)

    price_candidates = _extract_price_candidates(normalized_message)
    (
        current_price,
        original_price,
        used_price_fallback,
    ) = _select_current_and_original_price(price_candidates)
    product_price = current_price["value"] if current_price else None
    product_price_raw = current_price["raw"] if current_price else None
    original_price_value = original_price["value"] if original_price else None
    original_price_raw = original_price["raw"] if original_price else None

    coupon_code, coupon_text = _extract_coupon_data(normalized_message)
    description = _extract_description(normalized_message, urls)

    parse_status = "ok" if product_url else "partial_no_url"
    confidence = CONFIDENCE_BASE
    if product_url:
        confidence += CONFIDENCE_URL_WEIGHT
    if product_price is not None:
        confidence += CONFIDENCE_PRICE_WEIGHT
    if description:
        confidence += CONFIDENCE_DESCRIPTION_WEIGHT
    if coupon_code:
        confidence += CONFIDENCE_COUPON_WEIGHT
    if original_price_value is not None:
        confidence += CONFIDENCE_ORIGINAL_PRICE_WEIGHT
    if used_price_fallback:
        confidence -= 0.1

    return {
        "normalized_message": normalized_message,
        "all_urls": urls,
        "url_count": len(urls),
        "product_url": product_url,
        "product_domain": product_domain,
        "is_affiliate_url": affiliate_flag,
        "product_price": product_price,
        "product_price_raw": product_price_raw,
        "original_price": original_price_value,
        "original_price_raw": original_price_raw,
        "price_currency": "BRL" if product_price is not None else None,
        "coupon_code": coupon_code,
        "coupon_text": coupon_text,
        "product_description": description,
        "parse_status": parse_status,
        "parse_confidence": round(min(confidence, 1.0), 2),
        "schema_version": "v2",
    }
