#!/usr/bin/env python3
"""Audio-to-text transcription + trading strategy analysis using Groq APIs.

Transcribes audio files or YouTube/website URLs to text via Groq Whisper,
then optionally analyzes the transcript with an LLM to extract trading strategies.

Usage:
    python transcribe.py --url "https://www.youtube.com/watch?v=..."
    python transcribe.py --url "https://youtu.be/..."
    python transcribe.py "The ORB Strategy.mp3"
    python transcribe.py audio.wav
    python transcribe.py --no-analyze podcast.webm

Requirements:
    - GROQ_API_KEY in .env file (see .env.example)
    - ffmpeg installed and on PATH (for pydub audio splitting & yt-dlp)
    - pip install -r requirements.txt
"""

from __future__ import annotations

import argparse
import math
import os
import sys
import tempfile
from pathlib import Path

try:
    from dotenv import load_dotenv
    from groq import Groq
    from pydub import AudioSegment
    import yt_dlp
except ImportError as _imp_err:
    print(f"ERROR: Missing dependency — {_imp_err}")
    print("  Run: pip install -r requirements.txt")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SUPPORTED_FORMATS = {".mp3", ".wav", ".webm"}
MAX_FILE_SIZE_BYTES = 25 * 1024 * 1024  # 25 MB (Groq limit)
TIMESTAMP_INTERVAL_SEC = 60
WHISPER_MODEL = "whisper-large-v3"
LLM_MODEL = "llama-3.3-70b-versatile"
CHUNK_OVERLAP_MS = 2000  # 2 s overlap between chunks

SUMMARIES_DIR = Path(__file__).resolve().parent / "summaries"
DOWNLOADS_DIR = Path(__file__).resolve().parent / "downloads"

# Load .env early so FFMPEG_LOCATION is available at import time
load_dotenv(Path(__file__).resolve().parent / ".env")
load_dotenv()

# Locate ffmpeg — from .env or auto-detect common paths
FFMPEG_LOCATION: str | None = os.getenv("FFMPEG_LOCATION")
if not FFMPEG_LOCATION:
    _ffmpeg_name = "ffmpeg.exe" if sys.platform == "win32" else "ffmpeg"
    _common_paths: list[Path] = []
    if sys.platform == "win32":
        _common_paths = [Path(r"C:\ffmpeg\bin"), Path(r"C:\ProgramData\chocolatey\bin")]
    else:
        # macOS: Homebrew Apple Silicon, Homebrew Intel, system
        _common_paths = [Path("/opt/homebrew/bin"), Path("/usr/local/bin"), Path("/usr/bin")]
    # Also check user's home ffmpeg install
    _home_ffmpeg = Path.home() / "ffmpeg" / "bin"
    if _home_ffmpeg.exists():
        _common_paths.insert(0, _home_ffmpeg)
    for _candidate in _common_paths:
        if (_candidate / _ffmpeg_name).exists():
            FFMPEG_LOCATION = str(_candidate)
            break

# Add ffmpeg to PATH so pydub can find it too
if FFMPEG_LOCATION and FFMPEG_LOCATION not in os.environ.get("PATH", ""):
    os.environ["PATH"] = FFMPEG_LOCATION + os.pathsep + os.environ.get("PATH", "")

STRATEGY_ANALYSIS_PROMPT = """\
You are a trading strategy analyst. Given a transcript from a trading education
video, extract and summarize ALL trading strategies discussed.

For each strategy provide the following sections using EXACTLY this format:

=== TRADING STRATEGY ANALYSIS ===

Strategy Name: <name>
Source: <source file>

--- STRATEGY SUMMARY ---
<1-3 sentence overview>

--- ENTRY RULES ---
• <bullet points with exact conditions>

--- EXIT RULES ---
• <stop loss, take profit, time exit>

--- INDICATORS USED ---
• <list indicators and their parameters>

--- KEY PARAMETERS ---
• <parameter>: <value> (<description>)

--- SUITABILITY ASSESSMENT ---
• Best for: <market conditions>
• Avoid: <conditions to avoid>

Be specific and actionable. Use bullet points. If multiple strategies are
discussed, analyze each one separately with the same format.
"""

# ---------------------------------------------------------------------------
# API key
# ---------------------------------------------------------------------------


def load_api_key() -> str:
    """Load GROQ_API_KEY from .env file."""
    # Look for .env in the script's directory first, then cwd
    script_dir = Path(__file__).resolve().parent
    load_dotenv(script_dir / ".env")
    load_dotenv()  # fallback to cwd / parent .env files

    key = os.getenv("GROQ_API_KEY")
    if not key:
        raise RuntimeError(
            f"GROQ_API_KEY not found. Create a .env file in {script_dir} "
            "with: GROQ_API_KEY=your_key_here (see .env.example)"
        )
    return key


# ---------------------------------------------------------------------------
# URL download (YouTube, etc.)
# ---------------------------------------------------------------------------


def download_audio(url: str) -> Path:
    """Download audio from a YouTube or website URL via yt-dlp.

    Returns the path to the downloaded audio file.
    """
    DOWNLOADS_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Downloading audio from: {url}")

    ydl_opts: dict = {
        "format": "bestaudio/best",
        "outtmpl": str(DOWNLOADS_DIR / "%(title)s.%(ext)s"),
        "postprocessors": [
            {
                "key": "FFmpegExtractAudio",
                "preferredcodec": "mp3",
                "preferredquality": "128",
            }
        ],
        "quiet": True,
        "no_warnings": True,
    }
    if FFMPEG_LOCATION:
        ydl_opts["ffmpeg_location"] = FFMPEG_LOCATION

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=True)
            # yt-dlp changes extension to mp3 after postprocessing
            title = info.get("title", "audio")
            # Sanitize filename (yt-dlp does this internally too)
            safe_title = yt_dlp.utils.sanitize_filename(title)
            audio_path = DOWNLOADS_DIR / f"{safe_title}.mp3"

            if not audio_path.exists():
                # Fallback: find the most recent mp3 in downloads
                mp3s = sorted(DOWNLOADS_DIR.glob("*.mp3"), key=lambda p: p.stat().st_mtime)
                if mp3s:
                    audio_path = mp3s[-1]
                else:
                    raise RuntimeError("Download completed but audio file not found.")

            size_mb = audio_path.stat().st_size / (1024 * 1024)
            print(f"  Downloaded: {audio_path.name}  ({size_mb:.1f} MB)")
            return audio_path

    except RuntimeError:
        raise  # re-raise our own errors
    except Exception as e:
        hint = ""
        if "ffmpeg" in str(e).lower() or "ffprobe" in str(e).lower():
            hint = " (Hint: ffmpeg must be on PATH for audio extraction)"
        raise RuntimeError(f"Failed to download audio: {e}{hint}") from e


# ---------------------------------------------------------------------------
# Input validation
# ---------------------------------------------------------------------------


def validate_input(file_path: Path) -> None:
    """Validate the input audio file."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    if file_path.suffix.lower() not in SUPPORTED_FORMATS:
        raise ValueError(
            f"Unsupported format '{file_path.suffix}'. "
            f"Supported: {', '.join(sorted(SUPPORTED_FORMATS))}"
        )

    size_mb = file_path.stat().st_size / (1024 * 1024)
    print(f"Input: {file_path.name}  ({size_mb:.1f} MB)")


# ---------------------------------------------------------------------------
# Audio splitting
# ---------------------------------------------------------------------------


def split_audio(file_path: Path) -> list[tuple[Path, float]]:
    """Split an audio file into chunks that fit within Groq's size limit.

    Returns list of (chunk_path, start_offset_seconds) tuples.
    """
    try:
        audio = AudioSegment.from_file(str(file_path))
    except FileNotFoundError:
        raise RuntimeError(
            "ffmpeg not found. pydub requires ffmpeg to process audio. "
            "Install: brew install ffmpeg (macOS) or choco install ffmpeg (Windows) "
            "or sudo apt install ffmpeg (Linux)"
        )

    file_size = file_path.stat().st_size
    total_duration_ms = len(audio)

    if file_size <= MAX_FILE_SIZE_BYTES:
        # No splitting needed — return the original file
        return [(file_path, 0.0)]

    num_chunks = math.ceil(file_size / MAX_FILE_SIZE_BYTES)
    chunk_duration_ms = total_duration_ms // num_chunks

    print(f"File exceeds 25 MB — splitting into {num_chunks} chunks...")

    tmp_dir = tempfile.mkdtemp(prefix="transcribe_")
    chunks: list[tuple[Path, float]] = []

    for i in range(num_chunks):
        start_ms = i * chunk_duration_ms
        end_ms = min((i + 1) * chunk_duration_ms + CHUNK_OVERLAP_MS, total_duration_ms)
        chunk = audio[start_ms:end_ms]

        chunk_path = Path(tmp_dir) / f"chunk_{i:03d}.mp3"
        chunk.export(str(chunk_path), format="mp3", bitrate="128k")

        # If a single chunk is still too big, split it further
        if chunk_path.stat().st_size > MAX_FILE_SIZE_BYTES:
            sub_chunks = _split_chunk_further(chunk, start_ms, tmp_dir, i)
            chunks.extend(sub_chunks)
        else:
            chunks.append((chunk_path, start_ms / 1000.0))

    return chunks


def _split_chunk_further(
    audio: AudioSegment, base_offset_ms: int, tmp_dir: str, chunk_idx: int
) -> list[tuple[Path, float]]:
    """Recursively split a chunk that's still too large."""
    half = len(audio) // 2
    sub_chunks = []

    for j, (start, end) in enumerate([(0, half + CHUNK_OVERLAP_MS), (half, len(audio))]):
        sub = audio[start:min(end, len(audio))]
        path = Path(tmp_dir) / f"chunk_{chunk_idx:03d}_{j}.mp3"
        sub.export(str(path), format="mp3", bitrate="128k")
        sub_chunks.append((path, (base_offset_ms + start) / 1000.0))

    return sub_chunks


# ---------------------------------------------------------------------------
# Transcription
# ---------------------------------------------------------------------------


def transcribe_file(client: Groq, file_path: Path, language: str) -> object:
    """Transcribe a single audio file via Groq Whisper API."""
    with open(file_path, "rb") as f:
        response = client.audio.transcriptions.create(
            model=WHISPER_MODEL,
            file=f,
            response_format="verbose_json",
            timestamp_granularities=["segment"],
            language=language,
        )
    return response


def format_with_timestamps(
    segments: list, offset: float = 0.0, interval: int = TIMESTAMP_INTERVAL_SEC
) -> tuple[str, float]:
    """Format transcript segments with periodic [MM:SS] timestamp markers.

    Returns (formatted_text, last_global_end_seconds).
    """
    parts: list[str] = []
    next_ts_at = 0.0
    last_global_end = 0.0

    for seg in segments:
        seg_start = (seg.start if hasattr(seg, "start") else seg.get("start", 0)) + offset
        seg_end = (seg.end if hasattr(seg, "end") else seg.get("end", 0)) + offset
        seg_text = seg.text if hasattr(seg, "text") else seg.get("text", "")

        # Insert timestamp marker when crossing a boundary
        while seg_start >= next_ts_at:
            minutes = int(next_ts_at) // 60
            seconds = int(next_ts_at) % 60
            parts.append(f"\n[{minutes:02d}:{seconds:02d}]")
            next_ts_at += interval

        parts.append(seg_text.strip())
        last_global_end = seg_end

    return " ".join(parts).strip(), last_global_end


def transcribe_audio(
    client: Groq,
    file_path: Path,
    language: str = "en",
    include_timestamps: bool = True,
) -> str:
    """Transcribe an audio file, handling chunking if needed.

    Returns the full transcript text.
    """
    chunks = split_audio(file_path)
    total = len(chunks)
    all_parts: list[str] = []
    last_global_end = 0.0

    for i, (chunk_path, offset) in enumerate(chunks, 1):
        if total > 1:
            print(f"  Transcribing chunk {i} of {total}...")
        else:
            print("  Transcribing...")

        try:
            response = transcribe_file(client, chunk_path, language)
        except Exception as e:
            hint = ""
            if "rate" in str(e).lower() or "limit" in str(e).lower():
                hint = " (Hint: You may have hit a rate limit. Wait a moment and retry.)"
            raise RuntimeError(f"Groq API error on chunk {i}: {e}{hint}") from e

        segments = response.segments or []

        if not include_timestamps:
            # Just concatenate text
            text = " ".join(
                (s.text if hasattr(s, "text") else s.get("text", "")).strip()
                for s in segments
            )
            all_parts.append(text)
            continue

        # Filter out overlapping segments from previous chunk
        filtered = []
        for seg in segments:
            seg_start = (seg.start if hasattr(seg, "start") else seg.get("start", 0)) + offset
            if seg_start < last_global_end - 0.5:
                continue  # skip overlap
            filtered.append(seg)

        text, last_global_end = format_with_timestamps(filtered, offset)
        all_parts.append(text)

    return "\n".join(all_parts).strip()


# ---------------------------------------------------------------------------
# Strategy analysis (LLM)
# ---------------------------------------------------------------------------


def analyze_transcript(client: Groq, transcript: str, source_name: str) -> str:
    """Send transcript to Groq LLM for trading strategy extraction."""
    print("  Analyzing transcript for trading strategies...")

    prompt = f"Source file: {source_name}\n\nTranscript:\n{transcript}"

    # Truncate very long transcripts to fit context window
    max_chars = 120_000
    if len(prompt) > max_chars:
        prompt = prompt[:max_chars] + "\n\n[... transcript truncated for length ...]"

    try:
        response = client.chat.completions.create(
            model=LLM_MODEL,
            messages=[
                {"role": "system", "content": STRATEGY_ANALYSIS_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0.3,
            max_tokens=4096,
        )
        return response.choices[0].message.content
    except Exception as e:
        print(f"WARNING: LLM analysis failed: {e}")
        print("  The transcript was saved successfully — you can analyze it manually.")
        return ""


# ---------------------------------------------------------------------------
# File output
# ---------------------------------------------------------------------------


def save_transcript(text: str, audio_path: Path) -> Path:
    """Save transcript to summaries/ directory."""
    SUMMARIES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SUMMARIES_DIR / f"{audio_path.stem}.txt"
    out_path.write_text(text, encoding="utf-8")
    return out_path


def save_analysis(analysis: str, audio_path: Path) -> Path:
    """Save strategy analysis to summaries/ directory."""
    SUMMARIES_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SUMMARIES_DIR / f"{audio_path.stem}_analysis.txt"
    out_path.write_text(analysis, encoding="utf-8")
    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transcribe audio and extract trading strategies (Groq Whisper + LLM)",
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--url",
        type=str,
        help="YouTube or website URL to download audio from",
    )
    source.add_argument(
        "--file",
        type=str,
        dest="audio_file",
        help="Path to local audio file (MP3, WAV, or WEBM)",
    )
    parser.add_argument(
        "--language",
        type=str,
        default="en",
        help="Language code for transcription (default: en)",
    )
    parser.add_argument(
        "--no-timestamps",
        action="store_true",
        help="Disable [MM:SS] timestamp markers in transcript",
    )
    parser.add_argument(
        "--no-analyze",
        action="store_true",
        help="Skip LLM strategy analysis (transcribe only)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    try:
        # Resolve audio source: URL download or local file
        if args.url:
            audio_path = download_audio(args.url)
        else:
            audio_path = Path(args.audio_file).resolve()
            validate_input(audio_path)

        # Load API key and create client
        api_key = load_api_key()
        client = Groq(api_key=api_key)

        # Phase 1: Transcribe
        print("\n--- Phase 1: Transcription ---")
        transcript = transcribe_audio(
            client,
            audio_path,
            language=args.language,
            include_timestamps=not args.no_timestamps,
        )

        transcript_path = save_transcript(transcript, audio_path)
        print(f"  Transcript saved: {transcript_path}")

        # Phase 2: Strategy analysis
        if not args.no_analyze:
            print("\n--- Phase 2: Strategy Analysis ---")
            analysis = analyze_transcript(client, transcript, audio_path.name)
            if analysis:
                analysis_path = save_analysis(analysis, audio_path)
                print(f"  Analysis saved:   {analysis_path}")

        print("\nDone!")

    except (RuntimeError, FileNotFoundError, ValueError) as e:
        print(f"ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
