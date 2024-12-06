import re
from collections import Counter, OrderedDict
from typing import Iterable
import gzip
from bs4 import BeautifulSoup
from pandas import DataFrame, notnull, concat
from joblib import Parallel, delayed
import signal
from multiprocessing import TimeoutError

class Timeout:
    def __init__(self, seconds=600, error_message="Timeout occurred"):
        self.seconds = seconds
        self.error_message = error_message

    def __enter__(self):
        def handler(signum, frame):
            raise TimeoutError(self.error_message)
        signal.signal(signal.SIGALRM, handler)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)

HTML_FEATURE_COLUMNS = [
    "html_num_of_tags", "html_num_of_paragraphs", "html_num_of_divs", "html_num_of_titles",
    "html_num_of_external_js", "html_num_of_links", "html_num_of_scripts", "html_num_of_scripts_async",
    "html_num_of_scripts_type", "html_num_of_anchors", "html_num_of_anchors_to_hash",
    "html_num_of_anchors_to_https", "html_num_of_anchors_to_com", "html_num_of_inputs",
    "html_num_of_input_password", "html_num_of_hidden_elements", "html_num_of_input_hidden",
    "html_num_of_objects", "html_num_of_embeds", "html_num_of_frame", "html_num_of_iframe",
    "html_num_of_iframe_src", "html_num_of_iframe_src_https", "html_num_of_center", "html_num_of_imgs",
    "html_num_of_imgs_src", "html_num_of_meta", "html_num_of_links_href", "html_num_of_links_href_https",
    "html_num_of_links_href_css", "html_num_of_links_type", "html_num_of_link_type_app",
    "html_num_of_link_rel", "html_num_of_all_hrefs", "html_num_of_form_action", "html_num_of_form_http",
    "html_num_of_strong", "html_no_hrefs", "html_internal_href_ratio", "html_num_of_internal_hrefs",
    "html_external_href_ratio", "html_num_of_external_href", "html_num_of_icon", "html_icon_external",
    "html_num_of_form_php", "html_num_of_form_hash", "html_num_of_form_js", "html_malicious_form",
    "html_most_common", "html_num_of_css_internal", "html_num_of_css_external",
    "html_num_of_anchors_to_content", "html_num_of_anchors_to_void", "html_num_of_words",
    "html_num_of_lines", "html_unique_words", "html_average_word_len", "html_blocked_keywords_label",
    "html_num_of_blank_spaces", "html_create_element", "html_write", "html_char_code_at", "html_concat",
    "html_escape", "html_eval", "html_exec", "html_from_char_code", "html_link", "html_parse_int",
    "html_replace", "html_search", "html_substring", "html_unescape", "html_add_event_listener",
    "html_set_interval", "html_set_timeout", "html_push", "html_index_of", "html_document_write",
    "html_get", "html_find", "html_document_create_element", "html_window_set_timeout",
    "html_window_set_interval", "html_hex_encoding", "html_unicode_encoding", "html_long_variable_name"
]

def decompress_html(compressed_html: bytes) -> str:
    """
    Decompress a gzip-compressed HTML string and decode it to UTF-8.
    """
    try:
        if compressed_html:
            return gzip.decompress(compressed_html).decode('utf-8')
        else:
            return None
    except Exception as e:
        print(f"Error decompressing HTML: {e}")
        return None

_patterns = OrderedDict({
    "createElement": re.compile(r"(createElement\()"),
    "write": re.compile(r"(write\()"),
    "charCodeAt": re.compile(r"(charCodeAt\()"),
    "concat": re.compile(r"(concat\()"),
    "escape": re.compile(r"((?<!n)escape\()"),
    "eval": re.compile(r"(eval\()"),
    "exec": re.compile(r"(exec\()"),
    "fromCharCode": re.compile(r"(fromCharCode\()"),
    "link": re.compile(r"(link\()"),
    "parseInt": re.compile(r"(parseInt\()"),
    "replace": re.compile(r"(replace\()"),
    "search": re.compile(r"(search\()"),
    "substring": re.compile(r"(substring\()"),
    "unescape": re.compile(r"(unescape\()"),
    "addEventListener": re.compile(r"(addEventListener\()"),
    "setInterval": re.compile(r"(setInterval\()"),
    "setTimeout": re.compile(r"(setTimeout\()"),
    "push": re.compile(r"(push\()"),
    "indexOf": re.compile(r"(indexOf\()"),
    "documentWrite": re.compile(r"(document\.write\()"),
    "get": re.compile(r"(get\()"),
    "find": re.compile(r"(find\()"),
    "documentCreateElement": re.compile(r"(document\.createElement\()"),
    "windowSetTimeout": re.compile(r"(window\.setTimeout\()"),
    "windowSetInterval": re.compile(r"(window\.setInterval\()"),
    "hexEncoding": re.compile(r'\\x[0-9A-Fa-f]{2}'),
    "unicodeEncoding": re.compile(r'\\u[0-9A-Fa-f]{4}'),
    "longVariableName": re.compile(r'\b[a-zA-Z0-9_]{20,}\b')
})

_text_patterns = (
    re.compile(r"\b(suspended|blocked|forbidden|denied|restricted)\b", re.IGNORECASE),
    re.compile(r'\s{2,}')
)

def get_elements(soup: BeautifulSoup, tag=None, attr=None):
    return soup.find_all(tag, {attr: True}) if attr else soup.find_all(tag)

def get_tags_f(soup: BeautifulSoup) -> list:
    #print(soup)
    if not soup:
        return [-1] * 53
    tags = {tag.name: soup.find_all(tag.name) for tag in soup.find_all()}

    try:
        anchors = tags.get('a', [])
        hrefs = [a.get('href') for a in anchors if a.get('href')]
        hrefs_http = [href for href in hrefs if "http" in href]
        hrefs_internal = [href for href in hrefs if "http" not in href]
    except Exception as e:
        anchors, hrefs, hrefs_http, hrefs_internal = [], [], [], []

    no_hrefs_flag = len(hrefs) == 0
    external_hrefs_flag = len(hrefs_http) / len(hrefs) > 0.5 if hrefs else 0
    internal_hrefs_flag = len(hrefs_internal) / len(hrefs) <= 0.5 if hrefs else 0

    form_actions = get_elements(soup, 'form', 'action')
    malicious_form = any("http" in form.get('action', '') or
                            ".php" in form.get('action', '') or
                            "#" in form.get('action', '') or
                            "javascript:void" in form.get('action', '')
                            for form in form_actions)
    hidden_elements = [element for element in soup.find_all(True)
                        if (element.has_attr('hidden') or
                            'display: none' in element.get('style', '') or
                            'visibility: hidden' in element.get('style', '') or
                            'opacity: 0' in element.get('style', '') or
                            'position: absolute' in element.get('style', ''))]

    hidden_inputs = [input for input in soup.find_all('input')
                        if input.get('type') == 'hidden' or
                        'display: none' in input.get('style', '') or
                        'visibility: hidden' in input.get('style', '') or
                        'opacity: 0' in input.get('style', '') or
                        'position: absolute' in input.get('style', '')]

    return [len(tags), len(tags.get('p', [])), len(tags.get('div', [])), len(tags.get('title', [])),
            len(get_elements(soup, 'script', 'src')),
            len(get_elements(soup,'link')), len(tags.get('script', [])), len(get_elements(soup,'script', 'async')),
            len(get_elements(soup,'script', 'type')),
            len(anchors), len(get_elements(soup,'a', 'href="#"')),
            len([a for a in anchors if "http" in a.get('href', '')]),
            len([a for a in anchors if ".com" in a.get('href', '')]),
            len(tags.get('input', [])), len(get_elements(soup,'input', 'type="password"')), len(hidden_elements),
            len(hidden_inputs), len(tags.get('object', [])), len(tags.get('embed', [])),
            len(tags.get('frame', [])), len(tags.get('iframe', [])), len(get_elements(soup,'iframe', 'src')),
            len([iframe for iframe in get_elements(soup,'iframe', 'src') if "http" in iframe.get('src', '')]),
            len(tags.get('center', [])), len(tags.get('img', [])), len(get_elements(soup,'img', 'src')),
            len(tags.get('meta', [])), len(get_elements(soup,'link', 'href')),
            len([link for link in get_elements(soup,'link', 'href') if "http" in link.get('href', '')]),
            len([link for link in get_elements(soup,'link', 'href') if ".css" in link.get('href', '')]),
            len(get_elements(soup,'link', 'type')), len(get_elements(soup,'link', 'type="application/rss+xml"')),
            len(get_elements(soup,'link', 'rel="shortlink"')), len(soup.find_all(href=True)),
            len(form_actions), len([form for form in form_actions if "http" in form.get('action', '')]),
            len(tags.get('strong', [])), int(no_hrefs_flag), int(internal_hrefs_flag), len(hrefs_internal), int(external_hrefs_flag),
            len(hrefs_http), len(get_elements(soup,'link', 'rel="shortcut icon"')), int(bool(
            [icon for icon in get_elements(soup,'link', 'rel="shortcut icon"') if "http" in icon.get('href', '')])),
            len([form for form in form_actions if ".php" in form.get('action', '')]),
            len([form for form in form_actions if "#" in form.get('action', '')]),
            len([form for form in form_actions if
                    "javascript:void()" in form.get('action', '') or "javascript:void(0)" in form.get('action', '')]),
            int(malicious_form),
            Counter(hrefs).most_common(1)[0][1] / len(hrefs) if hrefs else 0,
            len([css for css in get_elements(soup,'link', 'rel="stylesheet"') if "http" not in css.get('href', '')]),
            len([css for css in get_elements(soup,'link', 'rel="stylesheet"') if "http" in css.get('href', '')]),
            len(get_elements(soup,'a', 'href="#content"')), len(get_elements(soup,'a', 'href="javascript:void(0)"'))
            ]


def get_text_f(html: str) -> tuple[int, int, int, float, int, int]:
    if html is None or html == 'None':
        return -1, -1, -1, -1, -1, -1
    try:
        words = html.split()
        html_num_of_words = len(words)
        html_num_of_lines = len(html.splitlines())
        html_unique_words = len(set(words))
        html_average_word_len = sum(len(word) for word in words) / len(words) if words else 0
    except Exception as e:
        html_num_of_words, html_num_of_lines, html_unique_words, html_average_word_len = 0, 0, 0, 0

    patterns = _text_patterns
    try:
        html_num_of_blank_spaces = len(patterns[1].findall(html))
    except Exception as e:
        html_num_of_blank_spaces = 0
    try:
        blocked_keywords_count = len(patterns[0].findall(html))
    except Exception as e:
        blocked_keywords_count = 0

    if blocked_keywords_count > 0:
        html_blocked_keywords_label = 1
    else:
        html_blocked_keywords_label = 0

    return (html_num_of_words, html_num_of_lines, html_unique_words, html_average_word_len,
            html_blocked_keywords_label, html_num_of_blank_spaces)


def get_js_f(js: list) -> Iterable[int | float]:
    if js is None:
        return[-1] * (len(_patterns))
    if not js:
        return [0] * (len(_patterns))

    regex_patterns = _patterns
    dic: dict[str, int | float] = {key: 0 for key in regex_patterns.keys()}

    for script in js:
        for key, pattern in regex_patterns.items():
            dic[key] += len(pattern.findall(str(script)))

    return dic.values()

def para_transform_chunk(chunk: DataFrame, chunk_index: int, timeout_seconds=440) -> DataFrame:
    try:
        with Timeout(timeout_seconds):
            print(f"[INFO] Processing chunk {chunk_index} with {len(chunk)} rows...")
            chunk['html_decompressed'] = chunk['html'].apply(
                lambda x: decompress_html(x['compressed_html']) if x and 'compressed_html' in x and x['compressed_html'] else None)
            chunk['soup'] = chunk['html_decompressed'].apply(lambda html: BeautifulSoup(html, 'html.parser') if notnull(html) else None)
            chunk['js_inline'] = chunk['soup'].apply(
                lambda soup: [script for script in soup.find_all('script') if not script.has_attr('src')] if soup else None)

            (chunk["html_num_of_tags"], chunk["html_num_of_paragraphs"], chunk["html_num_of_divs"], chunk["html_num_of_titles"],
                chunk["html_num_of_external_js"],
                chunk["html_num_of_links"], chunk["html_num_of_scripts"], chunk["html_num_of_scripts_async"],
                chunk["html_num_of_scripts_type"], chunk["html_num_of_anchors"],
                chunk["html_num_of_anchors_to_hash"], chunk["html_num_of_anchors_to_https"], chunk["html_num_of_anchors_to_com"],
                chunk["html_num_of_inputs"], chunk["html_num_of_input_password"],
                chunk["html_num_of_hidden_elements"], chunk["html_num_of_input_hidden"], chunk["html_num_of_objects"],
                chunk["html_num_of_embeds"], chunk["html_num_of_frame"],
                chunk["html_num_of_iframe"], chunk["html_num_of_iframe_src"], chunk["html_num_of_iframe_src_https"],
                chunk["html_num_of_center"], chunk["html_num_of_imgs"],
                chunk["html_num_of_imgs_src"], chunk["html_num_of_meta"], chunk["html_num_of_links_href"],
                chunk["html_num_of_links_href_https"], chunk["html_num_of_links_href_css"],
                chunk["html_num_of_links_type"], chunk["html_num_of_link_type_app"], chunk["html_num_of_link_rel"],
                chunk["html_num_of_all_hrefs"], chunk["html_num_of_form_action"],
                chunk["html_num_of_form_http"], chunk["html_num_of_strong"], chunk["html_no_hrefs"], chunk["html_internal_href_ratio"],
                chunk["html_num_of_internal_hrefs"],
                chunk["html_external_href_ratio"], chunk["html_num_of_external_href"], chunk["html_num_of_icon"],
                chunk["html_icon_external"], chunk["html_num_of_form_php"],
                chunk["html_num_of_form_hash"], chunk["html_num_of_form_js"], chunk["html_malicious_form"], chunk["html_most_common"],
                chunk["html_num_of_css_internal"], chunk["html_num_of_css_external"],
                chunk["html_num_of_anchors_to_content"], chunk["html_num_of_anchors_to_void"]) = zip(
                *chunk["soup"].apply(get_tags_f))

            chunk["html_num_of_words"], chunk["html_num_of_lines"], chunk["html_unique_words"], chunk["html_average_word_len"], chunk[
                "html_blocked_keywords_label"], chunk["html_num_of_blank_spaces"] = zip(*chunk["html_decompressed"].apply(get_text_f))

            (chunk["html_create_element"], chunk["html_write"], chunk["html_char_code_at"], chunk["html_concat"], chunk["html_escape"],
                chunk["html_eval"],
                chunk["html_exec"], chunk["html_from_char_code"], chunk["html_link"], chunk["html_parse_int"], chunk["html_replace"],
                chunk["html_search"],
                chunk["html_substring"], chunk["html_unescape"], chunk["html_add_event_listener"], chunk["html_set_interval"],
                chunk["html_set_timeout"],
                chunk["html_push"], chunk["html_index_of"], chunk["html_document_write"], chunk["html_get"], chunk["html_find"],
                chunk["html_document_create_element"],
                chunk["html_window_set_timeout"], chunk["html_window_set_interval"], chunk["html_hex_encoding"],
                chunk["html_unicode_encoding"],
                chunk["html_long_variable_name"]) = zip(*chunk["js_inline"].apply(get_js_f))

            # Drop intermediate columns to free memory
            chunk.drop(columns=["html_decompressed", "soup", "js_inline"], inplace=True)
            print(f"[INFO] Finished processing chunk {chunk_index}.")
            return chunk
    except TimeoutError:
        print(f"[WARN] Chunk {chunk_index} timed out. Filling HTML features with -1.")
        chunk[HTML_FEATURE_COLUMNS] = -1
        try:
            chunk.drop(columns=["html_decompressed", "soup", "js_inline"], errors="ignore", inplace=True)
            return chunk
        except:
            print("NO SUCH COLS")
            return chunk

#optimize the function parameters based on the system parameters
def transform_html(df: DataFrame, chunk_size=200, n_jobs=11, timeout_seconds=440) -> DataFrame:
    # Split the DataFrame into chunks
    num_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size != 0 else 0)
    chunks = [df.iloc[i * chunk_size: (i + 1) * chunk_size].copy() for i in range(num_chunks)]
    print("Number of chunks: ",len(chunks))
    processed_chunks = Parallel(n_jobs=n_jobs, backend='loky')(
        delayed(para_transform_chunk)(chunk, idx + 1, timeout_seconds) for idx, chunk in enumerate(chunks)
    )

    concatenated_df = concat(processed_chunks, ignore_index=True)

    if 'html' in concatenated_df.columns:
        concatenated_df = concatenated_df.drop(columns=['html'])
    
    return concatenated_df
