"""
IEWC → Qdrant Unified Crawler (Azure Function Compatible)
HTML + Attachment (PDF/TXT) → Unified schema ingestion
"""

import os, re, io, time, hashlib, pathlib, logging, pdfplumber
from typing import List, Dict, Optional, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
from urllib.parse import urljoin, urlparse, urlunparse, unquote

import requests
from bs4 import BeautifulSoup
try:
    from tqdm import tqdm
except ImportError:
    # Fallback if tqdm is not available
    def tqdm(iterable, *args, **kwargs):
        return iterable
from dotenv import load_dotenv
try:
    from pypdf import PdfReader
except ImportError:
    # Fallback to PyPDF2 for older installations
    try:
        from PyPDF2 import PdfReader
    except ImportError:
        PdfReader = None  # Will raise error if used without library

try:
    from sentence_transformers import SentenceTransformer
except ImportError:
    SentenceTransformer = None  # Will raise error if used without library

try:
    from qdrant_client import QdrantClient
    from qdrant_client.http import models as qmodels
except ImportError:
    QdrantClient = None
    qmodels = None  # Will raise error if used without library

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None  # Will raise error if used without library

# ---------------- CONFIG -----------------
# Try to load .env file if it exists (for local development)
try:
    load_dotenv()
except Exception:
    pass  # Azure Functions doesn't need .env file

DEFAULT_BASE_URL = os.environ.get("BASE_URL", "https://www.iewc.com/resources")
DEFAULT_MODEL = os.environ.get("EMBEDDING_MODEL", "text-embedding-3-small")
USER_AGENT = "IEWC-UniversalBot/6.0"
REQUEST_TIMEOUT = 20
THREADS = 20
SUPPORTED_ATTACH_EXT = {".pdf", ".txt"}

# ---------------- UTILS ------------------

def normalizeUrl(u: str) -> str:
    """
    Normalize a URL for identity hashing so that minor differences
    (trailing slash, query params, fragments, case) don't create new IDs.
    - Drops query and fragment
    - Lowercases scheme/host
    - Strips trailing slash on path (but keeps root '/')
    """
    pu = urlparse(u)
    scheme = pu.scheme.lower()
    netloc = pu.netloc.lower()
    path = pu.path or "/"
    if path != "/":
        path = path.rstrip("/")
        if not path:
            path = "/"
    # Drop params, query, fragment for identity
    nu = urlunparse((scheme, netloc, path, "", "", ""))
    return nu

def nowIso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

# ---------------- HTTP -------------------

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

def httpGet(url: str) -> Optional[str]:
    """Fetch HTML content from URL, returning None on failure."""
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        content_type = r.headers.get("content-type", "").lower()
        if r.status_code == 200 and "text/html" in content_type:
            return r.text
    except requests.exceptions.RequestException as e:
        logging.warning(f"Failed to fetch {url}: {e}")
    except Exception as e:
        logging.warning(f"Unexpected error fetching {url}: {e}")
    return None

def downloadBytes(url: str) -> Tuple[bytes, Dict[str, str]]:
    """Download bytes from URL with proper error handling."""
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r.content, {k.lower(): v for k, v in r.headers.items()}
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to download {url}: {e}")
        raise

# ---------------- TEXT EXTRACTION ----------------

def extractAiFriendlyMarkdown(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script","style","nav","footer","header","noscript","svg"]):
        tag.decompose()

    output = []

    if soup.title:
        # output.append(f"# {soup.title.get_text(strip=True)}\n")
        title_text = soup.title.get_text(strip=True)

    def parse_table(tbl):
        rows = []
        for tr in tbl.find_all("tr"):
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th","td"])]
            if cells:
                rows.append(cells)

        if len(rows) < 2:
            return ""

        header = "| " + " | ".join(rows[0]) + " |"
        separator = "| " + " | ".join(["---"] * len(rows[0])) + " |"
        body = "\n".join("| " + " | ".join(r) + " |" for r in rows[1:])

        return "\n".join([header, separator, body])

    for el in soup.find_all(["h1","h2","h3","p","table","ul"]):
        if el.name.startswith("h"):
            level = int(el.name[1])
            output.append(f"\n{'#' * level} {el.get_text(strip=True)}\n")

        elif el.name == "p":
            text = el.get_text(" ", strip=True)
            if text:
                output.append(text)

        elif el.name == "ul":
            for li in el.find_all("li"):
                output.append(f"- {li.get_text(strip=True)}")

        elif el.name == "table":
            table_md = parse_table(el)
            if table_md:
                output.append("\n" + table_md + "\n")

    return "\n".join(output).strip()


def chunkMarkdownText(
    text: str,
    max_chars: int = 2000,
    overlap: int = 200,
    min_chars: int = 200,
) -> List[str]:
    """
    Chunk markdown-like text into retrieval chunks.

    Rules:
    - Headings split sections.
    - TABLE sections are ATOMIC: never split, never merged with text, and bypass min_chars.
    - TEXT sections can be merged together up to max_chars (but never across tables).
    - Very large TEXT sections are window-split with overlap.
    """
    text = (text or "").strip()
    if not text:
        return []

    sections = re.split(r"\n(?=#+\s)", text)
    chunks: List[str] = []

    text_buffer = ""

    def flush_text_buffer():
        nonlocal text_buffer
        if text_buffer.strip() and isValidChunk(text_buffer.strip(), min_chars):
            chunks.append(text_buffer.strip())
        text_buffer = ""

    def is_table_section(section: str) -> bool:
        lines = [l for l in section.splitlines() if l.strip()]
        if not lines:
            return False

        pipe_lines = [l for l in lines if l.lstrip().startswith("|")]
        # markdown table signature: header + separator + at least one row
        md_table_like = any("| ---" in l or l.strip().startswith("|---") for l in lines) and len(pipe_lines) >= 2

        # your existing heuristics + table headings
        return (
            md_table_like
            or any("## Data Table" in l for l in lines)
            or any("ICEA Paired" in l for l in lines)
            or any("Telephone Paired" in l for l in lines)
            or any("Per NEMA & ICEA Method" in l for l in lines)
            or any("Color Codes" in l for l in lines)
            or any("Properties of Common Thermoplastic Compounds" in l for l in lines)
            or any("Properties of Common Thermoset Compounds" in l for l in lines)
            or any(l.lstrip().startswith("|") for l in lines[:6])  # quick “pipe-heavy” check
        )

    for section in sections:
        section = (section or "").strip()
        if not section:
            continue

        if is_table_section(section):
            # never mix tables with text
            flush_text_buffer()

            # table chunks are atomic; allow even if < min_chars
            # (just ensure it contains something besides headings)
            lines = [l.strip() for l in section.splitlines() if l.strip()]
            non_heading = [l for l in lines if not l.startswith("#")]
            if non_heading:
                chunks.append(section)
            continue

        # TEXT SECTION
        if len(section) <= max_chars:
            # merge text sections to reach min_chars and stay within max_chars
            candidate = (text_buffer + "\n\n" + section).strip() if text_buffer else section
            if len(candidate) <= max_chars:
                text_buffer = candidate
            else:
                flush_text_buffer()
                text_buffer = section
            continue

        # Large TEXT SECTION -> window split (never overlap if overlap >= max_chars)
        flush_text_buffer()
        start = 0
        step = max(1, max_chars - overlap)
        while start < len(section):
            end = start + max_chars
            chunk = section[start:end].strip()
            if isValidChunk(chunk, min_chars):
                chunks.append(chunk)
            start += step

    flush_text_buffer()
    return chunks




def pointIdFromUrlAndChunk(url: str, chunk_index: int) -> int:
    key = f"{normalizeUrl(url)}::chunk::{chunk_index}"
    return int(hashlib.md5(key.encode()).hexdigest()[:12], 16)

def isValidChunk(chunk: str, min_chars: int = 200) -> bool:
    if len(chunk) < min_chars:
        return False

    # Must contain non-heading text
    lines = [l.strip() for l in chunk.splitlines() if l.strip()]
    non_heading_lines = [l for l in lines if not l.startswith("#")]

    if not non_heading_lines:
        return False

    return True


def extractPdfTitle(pdf_bytes: bytes, fallback_title: str = "") -> str:
    """
    Extract the title from a PDF by checking:
    1. PDF metadata (Title field)
    2. First page content (first significant line/heading)
    3. Fallback to provided title or empty string
    
    Returns the best available title.
    """
    try:
        # Try to get title from PDF metadata using pypdf
        if PdfReader:
            try:
                reader = PdfReader(io.BytesIO(pdf_bytes))
                metadata = reader.metadata
                if metadata and metadata.get("/Title"):
                    title = metadata.get("/Title")
                    if title and title.strip():
                        # Remove common prefixes/suffixes
                        title = title.strip()
                        # Remove PDF encoding artifacts like (PDF) or similar
                        title = re.sub(r'\s*\(PDF\)\s*', '', title, flags=re.I)
                        if title:
                            return title
            except Exception:
                pass
        
        # Try to get title from first page content using pdfplumber
        try:
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                if len(pdf.pages) > 0:
                    first_page = pdf.pages[0]
                    page_text = first_page.extract_text()
                    
                    if page_text:
                        lines = [line.strip() for line in page_text.split("\n") if line.strip()]
                        
                        # Look for title-like lines (usually first few lines, not too long, not all caps unless short)
                        for line in lines[:10]:  # Check first 10 lines
                            # Skip very short lines, page numbers, dates, etc.
                            if len(line) < 3:
                                continue
                            # Skip lines that look like page numbers or dates
                            if re.match(r'^\d+$|^\d+/\d+/\d+$|^Page \d+$', line, re.I):
                                continue
                            # Skip lines that are all caps and very long (likely headers/footers)
                            if line.isupper() and len(line) > 100:
                                continue
                            # Prefer lines that are title-like (not too long, meaningful)
                            if 5 <= len(line) <= 200:
                                # Clean up the line
                                title = line.strip()
                                # Remove common prefixes
                                title = re.sub(r'^(CATALOG|PRODUCT CATALOG|CATALOGUE)\s*:?\s*', '', title, flags=re.I)
                                if title:
                                    return title
                        
                        # Fallback: use first substantial line
                        for line in lines[:5]:
                            if len(line) >= 10 and len(line) <= 200:
                                return line.strip()
        except Exception:
            pass
        
    except Exception as e:
        logging.debug(f"Failed to extract PDF title: {e}")
    
    # Fallback to provided title or empty string
    return fallback_title if fallback_title else ""


def extractPdfContent(pdf_bytes: bytes):
    all_text: List[str] = []
    all_tables: List[Dict] = []

    table_settings = {
        "vertical_strategy": "text",
        "horizontal_strategy": "lines",
        "snap_tolerance": 3,
        "join_tolerance": 3,
        "intersection_tolerance": 5,
        "text_tolerance": 3,
        "min_words_vertical": 2,
        "min_words_horizontal": 1,
    }

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):

            found = page.find_tables(table_settings=table_settings)

            good_bboxes = []
            for idx, t in enumerate(found, start=1):
                rows = t.extract()
                if rows and len(rows) >= 2:
                    all_tables.append({
                        "page": page_number,
                        "table_number": idx,
                        "table": rows
                    })
                    good_bboxes.append(t.bbox)

            # Extract non-table text only (prevents table text polluting text chunks)
            text_page = page
            for bbox in good_bboxes:
                text_page = text_page.outside_bbox(bbox)

            txt = text_page.extract_text(layout=True)
            if txt and txt.strip():
                all_text.append(txt.strip())

    return {
        "text": "\n\n".join(all_text).strip(),
        "tables": all_tables
    }


def extractPdfAiFriendlyMarkdown(pdf_bytes: bytes, title: str) -> str:
    pdf_data = extractPdfContent(pdf_bytes)

    full_text = pdf_data.get("text") or ""
    output: List[str] = []

    # ----- Title -----
    if title:
        output.append(f"# {title}\n")

    # ----- Text first (non-table text only) -----
    if full_text.strip():
        output.append(full_text.strip())

    # ----- Tables as markdown blocks (each will become its own chunk) -----
    def _esc(x) -> str:
        return ("" if x is None else str(x)).replace("\n", " ").replace("|", r"\|").strip()

    for t in pdf_data.get("tables", []):
        rows = t.get("table") or []
        if len(rows) < 2:
            continue

        output.append(f"\n## Data Table (Page {t['page']}, Table {t['table_number']})\n")

        header = [_esc(c) for c in rows[0]]
        output.append("| " + " | ".join(header) + " |")
        output.append("| " + " | ".join(["---"] * len(header)) + " |")

        for row in rows[1:]:
            safe = [_esc(c) for c in row]
            output.append("| " + " | ".join(safe) + " |")

    # ----- SPECIAL HANDLING blocks (your old code, now reachable) -----
    def _build_awg_mm2_table(raw_text: str) -> Optional[str]:
        idx = raw_text.find("AWG to mm2")
        if idx == -1:
            return None

        sub = raw_text[idx:]
        lines = [l.strip() for l in sub.splitlines() if l.strip()]

        start_idx = None
        for i, l in enumerate(lines):
            if l.startswith("AWG to mm2"):
                start_idx = i
                break
        if start_idx is None:
            return None

        pairs: List[Tuple[str, str]] = []
        numeric_mm2 = re.compile(r"^[0-9]+(\.[0-9]+)?$")

        for l in lines[start_idx + 1:]:
            if "Automotive SAE Recommended Conductors" in l:
                break
            parts = l.split()
            if len(parts) < 2:
                continue
            for i in range(0, len(parts) - 1, 2):
                awg = parts[i]
                mm2 = parts[i + 1]
                if not numeric_mm2.match(mm2):
                    continue
                pairs.append((awg, mm2))

        if not pairs:
            return None

        out = ["| AWG / MCM | mm2 |", "| --- | --- |"]
        for awg, mm2 in pairs:
            out.append(f"| {awg} | {mm2} |")
        return "\n".join(out)

    def _build_automotive_sae_table(raw_text: str) -> Optional[str]:
        idx = raw_text.find("Automotive SAE Recommended Conductors")
        if idx == -1:
            return None

        sub = raw_text[idx:]
        lines = [l.strip() for l in sub.splitlines() if l.strip()]

        start_idx = None
        for i, l in enumerate(lines):
            if l.startswith("AWG") and "Nominal OD of Strand" in l:
                start_idx = i
                break
        if start_idx is None:
            return None

        rows: List[Tuple[str, str, str]] = []
        for l in lines[start_idx + 1:]:
            if l.startswith("IEWC "):
                break
            parts = l.split()
            if len(parts) < 3:
                continue
            rows.append((parts[0], parts[1], parts[2]))

        if not rows:
            return None

        out = [
            "| AWG | # of Strands | Nominal OD of Strand (in) |",
            "| --- | --- | --- |",
        ]
        for awg, strands, nominal_od in rows:
            out.append(f"| {awg} | {strands} | {nominal_od} |")
        return "\n".join(out)

    def _build_resistance_table(raw_text: str, marker: str, heading: str) -> Optional[str]:
        idx = raw_text.find(marker)
        if idx == -1:
            return None

        sub = raw_text[idx:]
        lines = [l.strip() for l in sub.splitlines() if l.strip()]
        if not lines:
            return None

        header_tokens = lines[0].split()
        if len(header_tokens) < 3:
            return None

        materials = header_tokens[2:]

        rows: List[Tuple[str, List[str]]] = []
        for l in lines[1:]:
            if "Properties of Common Thermoset Compounds" in l or "Properties of Common Thermoplastic Compounds" in l:
                break
            parts = l.split()
            if len(parts) < 2:
                continue
            rows.append((parts[0], parts[1:]))

        if not rows:
            return None

        col_headers = ["Property"] + materials
        out = []
        out.append("| " + " | ".join(col_headers) + " |")
        out.append("| " + " | ".join(["---"] * len(col_headers)) + " |")

        for prop_name, values in rows:
            vals = values[:len(materials)]
            if len(vals) < len(materials):
                vals += [""] * (len(materials) - len(vals))
            out.append("| " + " | ".join([prop_name] + vals) + " |")

        return "\n".join(out)

    awg_mm2_table = _build_awg_mm2_table(full_text)
    if awg_mm2_table:
        output.append("\n## AWG to mm2 Conversion\n")
        output.append(awg_mm2_table)

    sae_table = _build_automotive_sae_table(full_text)
    if sae_table:
        output.append("\n## Automotive SAE Recommended Conductors\n")
        output.append(sae_table)

    thermo_plastic_heading = "Properties of Common Thermoplastic Compounds"
    thermo_plastic_table = _build_resistance_table(full_text, "Resistant To PVC", thermo_plastic_heading)
    if thermo_plastic_table:
        output.append(f"\n## {thermo_plastic_heading}\n")
        output.append(thermo_plastic_table)

    thermo_set_heading = "Properties of Common Thermoset Compounds"
    thermo_set_table = _build_resistance_table(full_text, "Resistant To SBR", thermo_set_heading)
    if thermo_set_table:
        output.append(f"\n## {thermo_set_heading}\n")
        output.append(thermo_set_table)

    return "\n".join(output).strip()




def extractTxtText(b: bytes) -> str:
    try:
        return b.decode("utf-8")
    except UnicodeDecodeError:
        return b.decode("latin-1", errors="ignore")
    


def extractTxtAiFriendlyMarkdown(txt_bytes: bytes, title: str) -> str:
    text = extractTxtText(txt_bytes)
    lines = text.splitlines()

    output = []

    if title:
        output.append(f"# {title}\n")

    for line in lines:
        line = line.strip()
        if not line:
            continue

        if line.isupper() and len(line) < 80:
            output.append(f"\n## {line}\n")

        elif line.endswith(":"):
            output.append(f"\n### {line[:-1]}\n")

        else:
            output.append(line)

    return "\n".join(output).strip()



def deriveTitle(url: str, html: Optional[str] = None) -> str:
    """Derive a readable title from HTML <title> when available, otherwise from the URL path.

    Examples:
    - HTML title 'Low Smoke Zero Halogen Compounds - IEWC' -> 'Low Smoke Zero Halogen Compounds'
    - URL '/resources/technical-guide/low-smoke-zero-halogen-compounds' -> 'low smoke zero halogen compounds'
    """
    # Prefer HTML <title> when present
    if html:
        try:
            soup = BeautifulSoup(html, "html.parser")
            if soup.title and soup.title.string:
                t = soup.title.string.strip()
                # Remove common separators and trailing site name (take left-most segment)
                for sep in ["|", "-", "—", "–", "·"]:
                    if sep in t:
                        t = t.split(sep)[0].strip()
                if t:
                    return t
        except Exception:
            pass

    # Fallback: derive from last path segment of the URL
    try:
        path = urlparse(url).path
        name = pathlib.Path(path).name
        if not name:
            return url
        name = unquote(name)
        # Strip file extension if present
        name = re.sub(r"\.[a-zA-Z0-9]{1,6}$", "", name)
        # Replace separators with spaces
        name = re.sub(r"[-_+]+", " ", name)
        # Remove leading numeric prefixes like '01-' and collapse spaces
        name = re.sub(r"^\d+\s*", "", name)
        name = re.sub(r"\s+", " ", name).strip()
        return name.lower()
    except Exception:
        return url

# ---------------- QDRANT ----------------

class QdrantIngestor:
    def __init__(self, collection: str, model_name: str):
        if QdrantClient is None:
            raise ImportError("qdrant_client library is not installed. Please install it with: pip install qdrant-client")
        
        qdrant_url = os.environ.get("QDRANT_URL")
        qdrant_api_key = os.environ.get("QDRANT_API_KEY") or None
        
        logging.info(f"Connecting to Qdrant at {qdrant_url}")
        try:
            self.client = QdrantClient(
                url=qdrant_url,
                api_key=qdrant_api_key
            )
            # Test connection
            self.client.get_collections()
            logging.info("✅ Successfully connected to Qdrant")
        except Exception as e:
            logging.error(f"Failed to connect to Qdrant at {qdrant_url}: {e}")
            raise
        
        self.collection = collection
        self.model_name = model_name

        # Simple ingestion statistics (per run)
        self.inserted_points: int = 0
        self.updated_points: int = 0
        self.unchanged_points: int = 0
        
        # Determine if using OpenAI or sentence-transformers
        self.use_openai = (
            model_name.startswith("text-embedding") or 
            "ada" in model_name.lower() or
            model_name.startswith("gpt")
        )
        
        if self.use_openai:
            if OpenAI is None:
                raise ImportError("openai library is not installed. Please install it with: pip install openai")
            openai_key = os.environ.get("OPENAI_API_KEY")
            if not openai_key:
                raise ValueError("OPENAI_API_KEY environment variable is required when using OpenAI models")
            self.openai_client = OpenAI(api_key=openai_key)
            # Default dimensions for OpenAI models
            if "text-embedding-3-small" in model_name:
                self.dim = 1536
            elif "text-embedding-3-large" in model_name:
                self.dim = 3072
            elif "text-embedding-ada-002" in model_name:
                self.dim = 1536
            else:
                self.dim = 1536  # default
        else:
            # Use sentence-transformers
            if SentenceTransformer is None:
                raise ImportError("sentence-transformers library is not installed. Please install it with: pip install sentence-transformers")
            try:
                self.st_model = SentenceTransformer(model_name)
                self.dim = self.st_model.get_sentence_embedding_dimension()
            except Exception as e:
                logging.error(f"Failed to load sentence-transformers model {model_name}: {e}")
                raise
        
        self._ensure_collection()

    def _ensure_collection(self):
        """Verify the Qdrant collection exists and has correct configuration."""
        try:
            cols = [c.name for c in self.client.get_collections().collections]
            if self.collection not in cols:
                raise ValueError(
                    f"Collection '{self.collection}' does not exist. "
                    f"Please create it manually with ({self.dim} Cosine) configuration."
                )
            
            # Verify collection configuration
            collection_info = self.client.get_collection(self.collection)
            vectors_config = collection_info.config.params.vectors
            
            # Determine if it's default vectors or named vectors
            # Default vectors can be represented as:
            # 1. VectorParams object directly
            # 2. Dict with empty string key: {"": VectorParams(...)}
            is_default_vector = False
            existing_dim = None
            
            if isinstance(vectors_config, dict):
                # Check if it's default vector (empty string key) or named vectors
                vector_keys = list(vectors_config.keys())
                if len(vector_keys) == 1 and vector_keys[0] == "":
                    # Default vector represented as {"": VectorParams(...)}
                    is_default_vector = True
                    existing_dim = vectors_config[""].size
                else:
                    # Named vectors - raise error
                    raise ValueError(
                        f"Collection '{self.collection}' uses named vectors. "
                        f"Please recreate it with default vectors ({self.dim} Cosine)."
                    )
            else:
                # VectorParams object directly - default vector
                is_default_vector = True
                existing_dim = vectors_config.size
            
            # Verify dimension matches
            if is_default_vector:
                if existing_dim != self.dim:
                    raise ValueError(
                        f"Collection '{self.collection}' has dimension {existing_dim}, "
                        f"but model requires {self.dim}. Please recreate the collection."
                    )
                else:
                    logging.info(f"Collection '{self.collection}' verified ({self.dim} Cosine)")
            
        except Exception as e:
            logging.error(f"Failed to verify collection '{self.collection}': {e}")
            raise

    # ---------- Helpers ----------
    def bumpTimestampOnly(self, pid: int, content_hash: Optional[str] = None):
        payload = {"updated": nowIso()}
        if content_hash is not None:
            payload["content_hash"] = content_hash
        try:
            self.client.set_payload(
                collection_name=self.collection,
                payload=payload,
                points=[pid],
                wait=True
            )
        except Exception as e:
            logging.warning(f"Failed to bump timestamp for {pid}: {e}")

    def _chunkText(self, text: str, max_chars: int = 8000) -> List[str]:
        """
        Split text into approximately sentence-aligned chunks if it's too long for
        a *single* embedding call. This is currently unused by the main pipeline,
        but kept as a utility for potential future use.
        """
        text = (text or "").strip()
        if not text:
            return []
        if len(text) <= max_chars:
            return [text]

        chunks: List[str] = []
        sentences = re.split(r"(?<=[.!?])\s+", text)
        current_chunk: List[str] = []
        current_length = 0

        for sentence in sentences:
            if not sentence:
                continue
            sentence_length = len(sentence)
            if current_chunk and current_length + sentence_length > max_chars:
                chunks.append(" ".join(current_chunk).strip())
                current_chunk = [sentence]
                current_length = sentence_length
            else:
                current_chunk.append(sentence)
                current_length += sentence_length + 1  # +1 for space

        if current_chunk:
            chunks.append(" ".join(current_chunk).strip())

        return chunks

    def getEmbedding(self, text: str) -> list:
        """Generate embedding using OpenAI or sentence-transformers based on model_name."""
        try:
            if self.use_openai:
                # OpenAI has token limits, truncate if needed
                # Rough estimate: 1 token ≈ 4 characters
                # max_chars = 8000  # Conservative limit for text-embedding-3-small
                max_chars = 12000  # SAFE limit to stay under 8192 tokens
                if len(text) > max_chars:
                    text = text[:max_chars]
                    logging.warning(f"Text truncated to {max_chars} characters for embedding")
                
                response = self.openai_client.embeddings.create(
                    input=text,
                    model=self.model_name
                )
                return response.data[0].embedding
            else:
                # Use sentence-transformers
                return self.st_model.encode(text, show_progress_bar=False).tolist()
        except Exception as e:
            logging.error(f"Embedding failed for text (length={len(text)}): {e}")
            raise  # Re-raise to handle upstream


    def upsertRecord(self, url: str, title: str, source_type: str, text: str):
        """
        Upsert a (possibly multi-chunk) record into Qdrant.

        - Splits long markdown text into logical chunks.
        - Uses a deterministic ID per (url, chunk_index).
        - Avoids unnecessary re-embedding via a content_hash check.
        """
        if not text or not text.strip():
            logging.warning(f"Skipping empty text for {url}")
            return

        # For PDFs we allow larger chunks and no overlap, while still respecting
        # the table-preserving logic in chunkMarkdownText.
        if source_type == "pdf":
            # chunks = chunkMarkdownText(text, max_chars=8000, overlap=0, min_chars=100)
            chunks = chunkMarkdownText(text, max_chars=8000, overlap=0, min_chars=100)
# (table chunks bypass min_chars now, so you can keep min_chars=100)

        else:
            chunks = chunkMarkdownText(text)
        if not chunks:
            logging.warning(f"No valid chunks produced for {url}")
            return

        total_chunks = len(chunks)
        norm_url = normalizeUrl(url)

        for idx, chunk in enumerate(chunks):
            try:
                pid = pointIdFromUrlAndChunk(norm_url, idx)
                content_hash = hashlib.sha256(chunk.encode("utf-8")).hexdigest()

                # Single retrieve to determine existence + previous hash
                try:
                    pts = self.client.retrieve(collection_name=self.collection, ids=[pid])
                except Exception as e:
                    logging.warning(f"Could not retrieve point {pid}: {e}")
                    pts = []

                existed = bool(pts)
                prev_hash = (pts[0].payload or {}).get("content_hash") if pts else None

                if existed and prev_hash == content_hash:
                    # Update timestamp & persist hash (if missing) without re-embedding
                    self.bumpTimestampOnly(pid, content_hash)
                    self.unchanged_points += 1
                    continue

                vector = self.getEmbedding(chunk)

                payload = {
                    "url": norm_url,
                    "title": title,
                    "sourceType": source_type,
                    "chunkIndex": idx,
                    "chunkCount": total_chunks,
                    "updated": nowIso(),
                    # "content_hash": content_hash,
                    "fullText": chunk,
                }

                self.client.upsert(
                    collection_name=self.collection,
                    points=[
                        qmodels.PointStruct(
                            id=pid,
                            vector=vector,
                            payload=payload,
                        )
                    ],
                    wait=True,
                )

                if existed:
                    self.updated_points += 1
                else:
                    self.inserted_points += 1

            except Exception as e:
                logging.error(f"Chunk {idx} failed for {url}: {e}", exc_info=True)


# ---------------- CRAWLING ----------------

def discoverInternalLinks(start_url: str, max_depth: int) -> Dict[str, str]:
    """
    Breadth-first discovery of internal links up to a maximum depth.

    Returns a mapping of URL -> HTML content, so callers can avoid
    re-downloading pages for processing.

    Only follows links that:
    - Stay under the original start_url prefix
    - Are not obvious binary/document assets (pdf, txt, doc/x, xls/xm, etc.)
    """
    visited: Set[str] = set()
    html_map: Dict[str, str] = {}
    to_visit: deque[Tuple[str, int]] = deque([(start_url, 0)])

    while to_visit:
        url, depth = to_visit.popleft()
        if url in visited or depth > max_depth:
            continue
        visited.add(url)
        html = httpGet(url)
        if html is None:
            continue
        html_map[url] = html
        if depth >= max_depth:
            continue
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            abs_link = urljoin(url, a["href"])
            if abs_link.startswith(start_url) and not re.search(r"\.(pdf|txt|docx?|xls[xm]?)$", abs_link, re.I):
                if abs_link not in visited:
                    to_visit.append((abs_link, depth + 1))
    return html_map

def findAttachments(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        absu = urljoin(base_url, a["href"])
        ext = pathlib.Path(urlparse(absu).path).suffix.lower()
        if ext in SUPPORTED_ATTACH_EXT:
            out.append(absu)
    return list(set(out))

# ---------------- MAIN PIPELINE ----------------

def processAttachment(ingestor: QdrantIngestor, attach_url: str, title: str):
    """Process and insert PDF or TXT attachment."""
    ext = pathlib.Path(urlparse(attach_url).path).suffix.lower()
    logging.info(f"Processing attachment: {attach_url} (type: {ext})")
    
    try:
        b, headers = downloadBytes(attach_url)
        logging.info(f"Downloaded {len(b)} bytes from {attach_url}")
    except Exception as e:
        logging.error(f"Failed to fetch attachment {attach_url}: {e}")
        return
    
    try:
        final_title = title or attach_url
        if ext == ".pdf":
            # Extract title from PDF itself (metadata or first page)
            pdf_title = extractPdfTitle(b, fallback_title=title or attach_url)
            if pdf_title and pdf_title != title:
                logging.info(f"Extracted PDF title: '{pdf_title}' (was: '{title}')")
            final_title = pdf_title
            text = extractPdfAiFriendlyMarkdown(b, pdf_title)
            stype = "pdf"
            logging.info(f"Extracted {len(text)} characters from PDF")
        elif ext == ".txt":
            # text = extract_txt_text(b)
            text = extractTxtAiFriendlyMarkdown(b, title or attach_url)
            stype = "txt"
            logging.info(f"Extracted {len(text)} characters from TXT")
        else:
            logging.warning(f"Unsupported file type: {ext}")
            return
        
        if text and text.strip():
            ingestor.upsertRecord(
                attach_url,
                final_title,
                stype,
                text
            )
            logging.info(f"✅ Successfully inserted/updated attachment: {attach_url}")
        else:
            logging.warning(f"Empty text extracted from {attach_url}")
    except Exception as e:
        logging.error(f"Failed to process attachment {attach_url}: {e}", exc_info=True)

def run(base_url: str, collection: str, max_depth: int, embedding_model: str):
    start = time.time()
    logging.info(f"Starting crawl for {base_url} (depth={max_depth})")
    ing = QdrantIngestor(collection, embedding_model)
    htmls = discoverInternalLinks(base_url, max_depth)
    urls = list(htmls.keys())
    logging.info(f"Discovered {len(urls)} URLs with HTML content")

    count_html = count_att = 0
    errors_html = errors_att = 0
    
    for u, h in tqdm(htmls.items(), desc="Processing"):
        # Derive title once for both HTML and attachments processing
        title = deriveTitle(u, h)
        
        try:
            text = extractAiFriendlyMarkdown(h)             # Best for AI Consumption
            if text.strip():
                ing.upsertRecord(u, title, "html", text)
                count_html += 1
                logging.debug(f"Processed HTML: {u}")
            else:
                logging.warning(f"Empty text extracted from {u}")
        except Exception as e:
            errors_html += 1
            logging.error(f"Failed to process HTML {u}: {e}", exc_info=True)
        
        # Process attachments
        try:
            attachments = findAttachments(h, u)
            logging.info(f"Found {len(attachments)} attachments on {u}")
            for att in attachments:
                try:
                    # pass the page-derived title to the attachment processing
                    processAttachment(ing, att, title=title)
                    count_att += 1
                except Exception as e:
                    errors_att += 1
                    logging.error(f"Failed to process attachment {att}: {e}", exc_info=True)
        except Exception as e:
            logging.error(f"Failed to find attachments on {u}: {e}", exc_info=True)

    elapsed = time.time() - start

    # High-level Qdrant write statistics for this run
    try:
        logging.info(
            "Qdrant write stats — inserted: %d, updated: %d, unchanged (timestamp only): %d",
            ing.inserted_points,
            ing.updated_points,
            ing.unchanged_points,
        )
    except Exception:
        # Keep ingestion robust even if stats logging fails for some reason
        pass
    
    # Verify data was inserted by checking collection count
    try:
        collection_info = ing.client.get_collection(ing.collection)
        point_count = getattr(collection_info, "points_count", None)
        if point_count is not None:
            logging.info(f"📊 Collection '{ing.collection}' now contains {point_count} points")
        else:
            logging.info(f"📊 Finished ingest for '{ing.collection}' (points_count not available via this client version)")
    except Exception as e:
        logging.warning(f"Could not verify collection count: {e}")
    
    logging.info(f"✅ Completed: {count_html} HTML pages, {count_att} attachments in {elapsed:.1f}s")
    if errors_html > 0 or errors_att > 0:
        logging.warning(f"⚠️ Errors: {errors_html} HTML, {errors_att} attachments")


# --------------- CLI / Azure Function Hook (optional) ---------------
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    base_url = os.environ.get("BASE_URL", DEFAULT_BASE_URL)
    collection = os.environ.get("QDRANT_COLLECTION", "iewc_unified")
    max_depth = int(os.environ.get("MAX_DEPTH", "2"))
    embedding_model = os.environ.get("EMBEDDING_MODEL", DEFAULT_MODEL)
    run(base_url, collection, max_depth, embedding_model)

