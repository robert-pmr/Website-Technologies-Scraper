import json
import re

def load_rules(path="tech_rules.json"):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

TECH_RULES = load_rules()

def unique_list(items):
    seen = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result

def get_confidence(proof_list):
    count = len(proof_list)
    if count == 1:
        return 0.60
    elif count == 2:
        return 0.80
    elif count >= 3:
        return 0.95
    return 0.0

def check_text_patterns(text_lower, patterns, source_name):
    proof = []
    for pattern in patterns:
        if pattern.lower() in text_lower:
            proof.append(f"{source_name} contains '{pattern}'")
    return proof

def check_url_patterns(urls_lower, patterns, source_name):
    proof = []
    for url in urls_lower:
        for pattern in patterns:
            if pattern.lower() in url:
                proof.append(f"{source_name} contains '{url}'")
    return proof

def check_meta_patterns(meta_contents, patterns):
    proof = []
    for content in meta_contents:
        for pattern in patterns:
            if pattern.lower() in content:
                proof.append(f"meta content contains '{pattern}'")
    return proof

def check_header_patterns(headers, patterns):
    proof = []
    for key, value in headers.items():
        header_text = f"{key}: {value}".lower()
        for pattern in patterns:
            if pattern.lower() in header_text:
                proof.append(f"header contains '{key}: {value}'")
    return proof

def check_cookie_patterns(cookie_names, patterns):
    proof = []
    for cookie_name in cookie_names:
        for pattern in patterns:
            if pattern.lower() in cookie_name.lower():
                proof.append(f"cookie name contains '{cookie_name}'")
    return proof

def check_regex_patterns(text, regex_patterns, source_name):
    proof = []
    for pattern in regex_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            proof.append(f"{source_name} matches regex '{pattern}'")
    return proof

def detect_technologies(
    html,
    headers,
    script_urls,
    link_urls,
    iframe_urls,
    meta_tags,
    cookies,
    inline_scripts,
    dom_markers
):
    technologies = []
    html_lower = html.lower()
    script_urls_lower = [s.lower() for s in script_urls if s]
    link_urls_lower = [l.lower() for l in link_urls if l]
    iframe_urls_lower = [i.lower() for i in iframe_urls if i]
    inline_scripts_lower = [s.lower() for s in inline_scripts if s]
    dom_markers_lower = [m.lower() for m in dom_markers if m]
    meta_contents = []
    for meta in meta_tags:
        content = meta.get("content", "")
        if content:
            meta_contents.append(content.lower())
    # compatibil și cu RequestsCookieJar, și cu dict simplu
    if hasattr(cookies, "keys"):
        cookie_names = list(cookies.keys())
    else:
        cookie_names = []
    scripts_combined = "\n".join(script_urls_lower)
    links_combined = "\n".join(link_urls_lower)
    iframes_combined = "\n".join(iframe_urls_lower)
    meta_combined = "\n".join(meta_contents)
    inline_scripts_combined = "\n".join(inline_scripts_lower)
    dom_markers_combined = "\n".join(dom_markers_lower)
    for rule in TECH_RULES:
        proof = []
        proof.extend(check_text_patterns(html_lower, rule.get("html", []), "html"))
        proof.extend(check_url_patterns(script_urls_lower, rule.get("scripts", []), "script src"))
        proof.extend(check_url_patterns(link_urls_lower, rule.get("links", []), "link href"))
        proof.extend(check_url_patterns(iframe_urls_lower, rule.get("iframes", []), "iframe src"))
        proof.extend(check_text_patterns(inline_scripts_combined, rule.get("scripts", []), "inline scripts"))
        proof.extend(check_text_patterns(dom_markers_combined, rule.get("dom", []), "dom markers"))
        proof.extend(check_meta_patterns(meta_contents, rule.get("meta", [])))
        proof.extend(check_header_patterns(headers, rule.get("headers", [])))
        proof.extend(check_cookie_patterns(cookie_names, rule.get("cookies", [])))
        proof.extend(check_regex_patterns(html, rule.get("regex_html", []), "html"))
        proof.extend(check_regex_patterns(scripts_combined, rule.get("regex_scripts", []), "scripts"))
        proof.extend(check_regex_patterns(links_combined, rule.get("regex_links", []), "links"))
        proof.extend(check_regex_patterns(iframes_combined, rule.get("regex_iframes", []), "iframes"))
        proof.extend(check_regex_patterns(meta_combined, rule.get("regex_meta", []), "meta"))
        proof.extend(check_regex_patterns(inline_scripts_combined, rule.get("regex_scripts", []), "inline scripts"))
        proof.extend(check_regex_patterns(dom_markers_combined, rule.get("regex_dom", []), "dom markers"))
        proof = unique_list(proof)
        min_proofs = rule.get("min_proofs", 1)
        if len(proof) >= min_proofs:
            technologies.append({
                "name": rule["name"],
                "category": rule["category"],
                "confidence": get_confidence(proof),
                "proof": proof
            })
    return technologies