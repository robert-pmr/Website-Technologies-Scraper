import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, urljoin
import pandas as pd
import requests
import urllib3
from bs4 import BeautifulSoup
from detector import detect_technologies

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

INPUT_PARQUET = "input.parquet"
OUTPUT_JSON = "output.json"
PARTIAL_OUTPUT_JSON = "output_partial.json"
COLUMN_NAME = "root_domain"
MAX_WORKERS = 20
TIMEOUT = 6
USER_AGENT = "Mozilla/5.0"
MAX_INTERNAL_PAGES = 2
KEYWORDS = [
    "about",
    "contact",
    "shop",
    "product",
    "products",
    "store",
    "cart",
    "checkout",
    "blog",
    "services",
    "service",
    "pricing",
    "book",
    "booking"
]

save_lock = threading.Lock()

def extract_domain(value: str) -> str:
    value = str(value).strip()
    if not value:
        return ""
    parsed = urlparse(value)
    domain = parsed.netloc or parsed.path
    return domain.strip().strip("/")

def load_domains_from_parquet(path: str, column_name: str) -> list[str]:
    df = pd.read_parquet(path)
    print("Coloane disponibile în parquet:", list(df.columns))
    if column_name not in df.columns:
        raise ValueError(
            f"Coloana '{column_name}' nu există în fișier. "
            f"Coloane găsite: {list(df.columns)}"
        )
    domains = (
        df[column_name]
        .dropna()
        .astype(str)
        .str.strip()
        .apply(extract_domain)
        .tolist()
    )
    cleaned_domains = []
    seen = set()
    for domain in domains:
        if domain and domain not in seen:
            seen.add(domain)
            cleaned_domains.append(domain)
    return cleaned_domains

def extract_page_data_from_soup(soup: BeautifulSoup) -> dict:
    script_urls = [
        script.get("src")
        for script in soup.find_all("script")
        if script.get("src")
    ]
    link_urls = [
        link.get("href")
        for link in soup.find_all("link")
        if link.get("href")
    ]
    iframe_urls = [
        iframe.get("src")
        for iframe in soup.find_all("iframe")
        if iframe.get("src")
    ]
    inline_scripts = [
        script.get_text(" ", strip=True)
        for script in soup.find_all("script")
        if script.get_text(" ", strip=True)
    ]
    meta_tags = soup.find_all("meta")
    dom_markers = []
    for tag in soup.find_all(True):
        tag_id = tag.get("id")
        if tag_id:
            dom_markers.append(tag_id)
        tag_classes = tag.get("class", [])
        if tag_classes:
            dom_markers.extend(tag_classes)
    return {
        "script_urls": script_urls,
        "link_urls": link_urls,
        "iframe_urls": iframe_urls,
        "inline_scripts": inline_scripts,
        "meta_tags": meta_tags,
        "dom_markers": dom_markers,
    }

def choose_internal_links(base_url: str, soup: BeautifulSoup, domain: str) -> list[str]:
    candidates = []
    seen = set()
    #candidate paths daca nu exista in homepage
    manual_candidates = [
        "/about",
        "/contact",
        "/shop",
        "/products"
    ]
    for path in manual_candidates:
        full_url = urljoin(base_url, path)
        parsed = urlparse(full_url)
        if domain in parsed.netloc and full_url not in seen:
            seen.add(full_url)
            candidates.append(full_url)
    # linkuri reale gasite in homepage
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href:
            continue
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        if domain not in parsed.netloc:
            continue
        normalized = parsed._replace(fragment="").geturl()
        lowered = normalized.lower()
        if any(keyword in lowered for keyword in KEYWORDS):
            if normalized not in seen:
                seen.add(normalized)
                candidates.append(normalized)
    return candidates[:MAX_INTERNAL_PAGES]

def merge_technologies(tech_lists: list[list[dict]]) -> list[dict]:
    merged = {}
    for tech_list in tech_lists:
        for tech in tech_list:
            name = tech["name"]
            if name not in merged:
                merged[name] = {
                    "name": tech["name"],
                    "category": tech["category"],
                    "confidence": tech["confidence"],
                    "proof": list(tech["proof"])
                }
            else:
                merged[name]["proof"].extend(tech["proof"])
                merged[name]["proof"] = list(dict.fromkeys(merged[name]["proof"]))
                if tech["confidence"] > merged[name]["confidence"]:
                    merged[name]["confidence"] = tech["confidence"]
    return sorted(merged.values(), key=lambda x: x["name"].lower())

def analyze_single_page(url: str, session: requests.Session) -> tuple[dict | None, BeautifulSoup | None]:
    response = session.get(
        url,
        timeout=TIMEOUT,
        headers={"User-Agent": USER_AGENT},
        verify=False
    )
    if response.status_code >= 400:
        return None, None
    soup = BeautifulSoup(response.text, "html.parser")
    page_data = extract_page_data_from_soup(soup)
    technologies = detect_technologies(
        html=response.text,
        headers=response.headers,
        script_urls=page_data["script_urls"],
        link_urls=page_data["link_urls"],
        iframe_urls=page_data["iframe_urls"],
        meta_tags=page_data["meta_tags"],
        cookies=response.cookies,
        inline_scripts=page_data["inline_scripts"],
        dom_markers=page_data["dom_markers"]
    )
    result = {
        "final_url": response.url,
        "status_code": response.status_code,
        "technologies": technologies
    }
    return result, soup

def analyze_domain(domain: str) -> dict:
    base_url = f"https://{domain}"
    session = requests.Session()
    try:
        homepage_result, homepage_soup = analyze_single_page(base_url, session)
        if homepage_result is None:
            return {
                "domain": domain,
                "final_url": base_url,
                "status_code": None,
                "error": "Could not load homepage",
                "technologies": []
            }
        all_tech_lists = [homepage_result["technologies"]]
        internal_links = choose_internal_links(
            homepage_result["final_url"],
            homepage_soup,
            domain
        )
        visited = {homepage_result["final_url"]}
        for internal_url in internal_links:
            if internal_url in visited:
                continue
            visited.add(internal_url)
            try:
                page_result, _ = analyze_single_page(internal_url, session)
                if page_result is not None:
                    all_tech_lists.append(page_result["technologies"])
            except Exception:
                pass
        merged_technologies = merge_technologies(all_tech_lists)
        return {
            "domain": domain,
            "final_url": homepage_result["final_url"],
            "status_code": homepage_result["status_code"],
            "error": None,
            "technologies": merged_technologies
        }
    except Exception as e:
        return {
            "domain": domain,
            "final_url": None,
            "status_code": None,
            "error": str(e),
            "technologies": []
        }

def save_results(results: list[dict], filename: str) -> None:
    with save_lock:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)

def print_summary(results):
    tech_set = set()
    for r in results:
        for t in r["technologies"]:
            tech_set.add(t["name"])
    print("\nTotal tehnologii unice:", len(tech_set))

def main() -> None:
    domains = load_domains_from_parquet(INPUT_PARQUET, COLUMN_NAME)
    total = len(domains)
    print(f"Am încărcat {total} domenii din {INPUT_PARQUET}")
    results = []
    completed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_domain = {
            executor.submit(analyze_domain, domain): domain
            for domain in domains
        }
        for future in as_completed(future_to_domain):
            result = future.result()
            results.append(result)
            completed += 1
            print(
                f"Verificate: {completed}/{total} | "
                f"{result['domain']} | "
                f"tech={len(result['technologies'])}"
            )
            save_results(results, PARTIAL_OUTPUT_JSON)
    order_map = {domain: i for i, domain in enumerate(domains)}
    results.sort(key=lambda x: order_map.get(x["domain"], 10**9))
    save_results(results, OUTPUT_JSON)
    print_summary(results)
    print(f"\nRezultatele finale au fost salvate în {OUTPUT_JSON} .")

if __name__ == "__main__":
    main()