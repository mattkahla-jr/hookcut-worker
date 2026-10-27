#!/usr/bin/env python3
"""HookCut render worker (host build) - v2 engine — adds live progress reporting.

Same engine as worker.py (silence-cut, single pass, loudnorm), but the job
row now carries progress (0-100) + stage so the web app can show a real
progress bar on the job card and in the top bar:

  2   downloading (fetching the raw upload)
  5   transcribing (C3: Deepgram word-level transcript, stored on the job)
  15  analyzing   (silence scan, mapped across the file duration)
  30  rendering   (ffmpeg -progress mapped to 30..92)
  95  uploading   (sending the finished cut back to storage)
  100 done

Polls hookcut_jobs for queued jobs, one per pass (the run_worker.sh loop
repeats it). Designed to run on the dedicated render host; configured
purely through the environment file below.
"""
import json
import os
import re
import subprocess
import tempfile
import threading
import time

ENV_PATH = os.environ.get("HOOKCUT_ENV_FILE", "")
BUCKET = "hookcut"
ENGINE_KIND = "render"


def load_env():
    env = {}
    if ENV_PATH and os.path.exists(ENV_PATH):
        for line in open(ENV_PATH):
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    for k in ("NEXT_PUBLIC_SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY",
              "ANTHROPIC_API_KEY", "DEEPGRAM_API_KEY"):
        if os.environ.get(k):
            env[k] = os.environ[k]
    return env


ENV = load_env()
SB_URL = ENV["NEXT_PUBLIC_SUPABASE_URL"]
SB_KEY = ENV["SUPABASE_SERVICE_ROLE_KEY"]


def sb_req(method, path, data=None, headers_extra=None, raw=False):
    """All Supabase traffic goes through curl: Python urllib reads get
    truncated by this VM's egress proxy (IncompleteRead).

    Torture-fix F1: JSON payloads are serialized to a temp file and
    sent with --data-binary @file, never as an inline -d argument:
    Linux caps a single argv entry at 128 KiB (MAX_ARG_STRLEN) and a
    long take's transcript PATCH body is larger - the inline form
    died with [Errno 7] Argument list too long AFTER Deepgram had
    billed, before editing. Raw bytes keep the stdin path they have
    always used (storage uploads/downloads). Torture-fix F4: every
    call carries --connect-timeout and --max-time so a stalled
    transfer fails the unit of work and the claim/reclaim machinery
    can move on, instead of holding a claim forever; storage
    transfers get the long bound (the gate measured ~700 s for an
    840 MB download), REST the short one. A failed call is logged
    with curl's stderr and raised as human copy (torture-fix F2: no
    raw subprocess output may reach a job's error field)."""
    url = f"{SB_URL}{path}"
    max_time = 3600 if path.startswith("/storage/v1/object/") else 300
    cmd = ["curl", "-s", "-X", method, url,
           "--connect-timeout", "15", "--max-time", str(max_time),
           "-H", f"apikey: {SB_KEY}",
           "-H", f"Authorization: Bearer {SB_KEY}", "-H", "Expect:"]
    infile = None
    tmp_path = None
    if data is not None:
        if isinstance(data, (dict, list)):
            fd, tmp_path = tempfile.mkstemp(prefix="sbreq-",
                                            suffix=".json")
            with os.fdopen(fd, "wb") as fh:
                fh.write(json.dumps(data).encode())
            cmd += ["-H", "Content-Type: application/json",
                    "--data-binary", f"@{tmp_path}"]
        else:
            infile = data
            cmd += ["--data-binary", "@-"]
    if headers_extra:
        for k, v in headers_extra.items():
            cmd += ["-H", f"{k}: {v}"]
    try:
        p = subprocess.run(cmd, input=infile, capture_output=True)
    finally:
        if tmp_path:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
    if p.returncode != 0:
        print(f"sb_req {method} {path}: curl rc={p.returncode}: "
              f"{p.stderr.decode(errors='replace')[:300]}")
        raise RuntimeError(
            "We hit a connection problem talking to our servers. "
            "Please try again.")
    b = p.stdout
    if raw:
        return b
    return json.loads(b) if b else None


def rest(method, path, payload=None):
    hdrs = {"Prefer": "return=representation"} if method in ("POST", "PATCH") else {}
    return sb_req(method, f"/rest/v1/{path}", payload, hdrs)


def set_progress(jid, pct, stage):
    try:
        rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
             {"progress": int(pct), "stage": stage})
    except Exception:  # noqa: BLE001 - progress is best-effort, never fail a job for it
        pass


def download(path, dest):
    # Stream straight to disk: loading a multi-hundred-MB source into
    # Python memory first contributed to worker OOM kills (Oct 25, 2026:
    # a 151 MB 4K upload killed the host twice mid-render).
    cmd = ["curl", "-s", "-f", "-o", dest,
           "--connect-timeout", "15", "--max-time", "3600",
           f"{SB_URL}/storage/v1/object/{BUCKET}/{path}",
           "-H", f"apikey: {SB_KEY}", "-H", f"Authorization: Bearer {SB_KEY}"]
    p = subprocess.run(cmd, capture_output=True)
    if p.returncode != 0:
        # stderr to the worker log only (torture-fix F2); timeouts
        # per torture-fix F4.
        print(f"download failed for {path}: rc={p.returncode}: "
              f"{p.stderr.decode(errors='replace')[:300]}")
        raise RuntimeError(
            "We couldn't download the original video. "
            "Please try again.")


def upload(path, src_file, content_type="video/mp4"):
    # Streamed from disk for the same reason as download().
    cmd = ["curl", "-s", "-f", "-X", "POST",
           "--connect-timeout", "15", "--max-time", "3600",
           f"{SB_URL}/storage/v1/object/{BUCKET}/{path}",
           "-H", f"apikey: {SB_KEY}", "-H", f"Authorization: Bearer {SB_KEY}",
           "-H", f"Content-Type: {content_type}", "-H", "x-upsert: true",
           "--data-binary", f"@{src_file}"]
    p = subprocess.run(cmd, capture_output=True)
    if p.returncode != 0:
        # stderr to the worker log only (torture-fix F2); timeouts
        # per torture-fix F4.
        print(f"upload failed for {path}: rc={p.returncode}: "
              f"{p.stderr.decode(errors='replace')[:300]}")
        raise RuntimeError(
            "We couldn't upload the finished video. "
            "Please try again.")


def probe_duration(path):
    out = subprocess.check_output(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "csv=p=0", path], text=True)
    return float(out.strip())


# ---------------------------------------------------------------------------
# C3: transcription (Deepgram pre-recorded API).
#
# The transcript is stored on the job row in `transcript_words` as one JSON
# object: {words, utterances, text, provider, model, language, duration,
# request_id, transcribed_at}. Every word's start/end/confidence is copied
# VERBATIM from Deepgram's response - never rounded, adjusted, or invented
# (briefing Hard Rule: no fabricated timestamps). Transcript content is job
# data only: it is never written to cut_events/analytics and never used for
# training (Hard Rule 5). A job that already has a transcript is never
# re-transcribed.
# ---------------------------------------------------------------------------

DEEPGRAM_KEY_FILE = os.environ.get(
    "DEEPGRAM_API_KEY_FILE",
    os.path.expanduser("~/.config/hooklab/deepgram_api_key"))
DEEPGRAM_MODEL = os.environ.get("DEEPGRAM_MODEL", "nova-2")


class TranscriptionError(Exception):
    """A transcription failure whose message is safe to show the user."""


def deepgram_key():
    """Resolve the Deepgram API key; the value is never logged or stored
    anywhere but Deepgram's auth header. Order: process environment (the
    Render service env), the worker env file, then the dedicated key file
    (~/.config/hooklab/deepgram_api_key, 600 perms, VM worker)."""
    k = os.environ.get("DEEPGRAM_API_KEY") or ENV.get("DEEPGRAM_API_KEY")
    if k:
        return k.strip()
    try:
        with open(DEEPGRAM_KEY_FILE) as f:
            return f.read().strip() or None
    except OSError:
        return None


def extract_audio(src, dest):
    p = subprocess.run(
        ["ffmpeg", "-y", "-v", "error", "-i", src, "-vn", "-ac", "1",
         "-ar", "16000", dest],
        capture_output=True, text=True)
    if p.returncode != 0 or not os.path.exists(dest) or os.path.getsize(dest) == 0:
        raise TranscriptionError(
            "We couldn't find an audio track in this video, so there's "
            "nothing to transcribe. Please upload a video with sound.")


def deepgram_transcribe(audio_path):
    """POST the audio to Deepgram and return the normalized transcript.
    Uses curl like all other worker egress (urllib reads get truncated by
    the proxy). Raises TranscriptionError with a plain-language message
    on any failure."""
    key = deepgram_key()
    if not key:
        raise TranscriptionError(
            "Transcription isn't configured on the worker yet (the "
            "Deepgram API key is missing). Please try again later.")
    params = ("model=" + DEEPGRAM_MODEL +
              "&smart_format=true&punctuate=true&utterances=true"
              "&filler_words=true&language=en&mip_opt_out=true")
    out_file = audio_path + ".dg.json"
    p = subprocess.run(
        ["curl", "-s", "-o", out_file, "-w", "%{http_code}",
         "--connect-timeout", "15", "--max-time", "600", "-X", "POST",
         "https://api.deepgram.com/v1/listen?" + params,
         "-H", f"Authorization: Token {key}",
         "-H", "Content-Type: audio/wav",
         "--data-binary", "@" + audio_path],
        capture_output=True, text=True)
    code = (p.stdout or "").strip()
    if code != "200":
        detail = ""
        try:
            with open(out_file) as f:
                detail = (json.load(f).get("err_msg") or "")[:120]
        except Exception:  # noqa: BLE001 - error body is best-effort only
            pass
        if code in ("401", "403"):
            raise TranscriptionError(
                "The transcription service rejected the worker's API key. "
                "Please try again later.")
        if code == "429":
            raise TranscriptionError(
                "The transcription service is busy right now. "
                "Please try again in a few minutes.")
        raise TranscriptionError(
            "Transcription failed (service returned "
            f"{code or 'no response'}" + (f": {detail}" if detail else "")
            + "). Please try again.")
    with open(out_file) as f:
        resp = json.load(f)
    results = resp.get("results") or {}
    channels = results.get("channels") or []
    alt = ((channels[0].get("alternatives") or [{}])[0]) if channels else {}
    # Verbatim copy of what Deepgram returned - no rounding or adjustment.
    words = [{
        "word": w.get("word"),
        "punctuated_word": w.get("punctuated_word") or w.get("word"),
        "start": w.get("start"),
        "end": w.get("end"),
        "confidence": w.get("confidence"),
    } for w in (alt.get("words") or [])]
    utterances = [{
        "start": u.get("start"),
        "end": u.get("end"),
        "confidence": u.get("confidence"),
        "transcript": u.get("transcript"),
    } for u in (results.get("utterances") or [])]
    meta = resp.get("metadata") or {}
    note_deepgram_billed(meta.get("duration"))
    model_name = None
    model_info = meta.get("model_info") or {}
    if model_info:
        model_name = next(iter(model_info.values())).get("name")
    from datetime import datetime, timezone
    return {
        "words": words,
        "utterances": utterances,
        "text": (alt.get("paragraphs") or {}).get("transcript")
                or alt.get("transcript") or "",
        "provider": "deepgram",
        "model": model_name or DEEPGRAM_MODEL,
        "language": (channels[0].get("detected_language")
                     if channels else None) or "en",
        "duration": meta.get("duration"),
        "request_id": meta.get("request_id"),
        "transcribed_at": datetime.now(timezone.utc).isoformat(),
    }


def transcribe_job(job, src, tmp):
    """C3 stage: status uploaded/queued -> transcribing -> (stored
    transcript). Idempotent: a job that already has transcript_words is
    never re-transcribed and Deepgram is not called again. Failures raise
    TranscriptionError; the caller marks the job failed - a job never
    sits stuck in 'transcribing' and never falls through to a silent
    silence-only cut."""
    jid = job["id"]
    if job.get("transcript_words"):
        return job["transcript_words"]
    rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
         {"status": "transcribing", "progress": 5, "stage": "transcribing"})
    audio = os.path.join(tmp, "audio.wav")
    extract_audio(src, audio)
    result = deepgram_transcribe(audio)
    if not result["words"]:
        raise TranscriptionError(
            "We couldn't hear any speech in this video, so there's "
            "nothing to transcribe. If someone is talking, check the "
            "audio is clear and try again.")
    rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
         {"transcript_words": result, "progress": 14,
          "stage": "transcribed"})
    print(f"job {jid}: transcribed {len(result['words'])} words "
          f"({result['model']}, request {result['request_id']})")
    return result


# ---------------------------------------------------------------------------
# Multi-Clip Jobs: N source files, ONE continuous take.
#
# A job's sources live in hookcut_job_sources (idx = play order); the
# job row's source_storage_key remains the FIRST source, and a job
# with no rows there is a one-clip job by definition (everything
# created before Multi-Clip). The pipeline treats the clips as one
# take on a STITCHED timeline: clip k occupies
# [offset_k, offset_k + duration_k), where offsets are cumulative
# sums of the ffprobe-measured clip durations. Every later stage
# (edit, captions, script match, render, export) works in stitched
# time exactly as it works in source time for a one-clip job; the
# source-clip dimension rides along as data (word.clip, EDL
# segment.clip, timeline source_clip) and is only resolved back to a
# file + local time at render, the one place media is read.
# ---------------------------------------------------------------------------

def job_sources(job):
    """The job's ordered source files: [{storage_key, filename,
    row_id}] from hookcut_job_sources when rows exist, else the
    one-clip fallback to the job row's own key."""
    rows = rest("GET",
                f"hookcut_job_sources?job_id=eq.{job['id']}"
                "&select=id,idx,storage_key,filename&order=idx.asc")
    if isinstance(rows, list) and rows:
        return [{"storage_key": r["storage_key"],
                 "filename": r.get("filename"),
                 "row_id": r.get("id")} for r in rows]
    return [{"storage_key": job["source_storage_key"],
             "filename": job.get("source_filename"), "row_id": None}]


def clip_offsets(durations):
    """Stitched-timeline start of each clip: cumulative sums of the
    measured durations (clip 0 starts at 0)."""
    offs, acc = [], 0.0
    for d in durations:
        offs.append(acc)
        acc += d
    return offs


def stitch_transcripts(parts, durations):
    """Join per-clip Deepgram transcripts into ONE word stream on the
    stitched timeline. Word and utterance times inside a clip are
    copied verbatim from Deepgram; the only arithmetic is adding the
    clip's offset (a sum of measured durations - Hard Rule: no
    fabricated timestamps). Every word and utterance carries its
    source clip index, and the per-clip records are kept under
    "clips" so later stages know where the camera restarts are."""
    from datetime import datetime, timezone
    words, utterances, texts, clips_meta = [], [], [], []
    offset = 0.0
    for k, (part, dur) in enumerate(zip(parts, durations)):
        for w in part["words"]:
            w2 = dict(w)
            w2["start"] = w["start"] + offset
            w2["end"] = w["end"] + offset
            w2["clip"] = k
            words.append(w2)
        for u in part["utterances"]:
            u2 = dict(u)
            u2["start"] = u["start"] + offset
            u2["end"] = u["end"] + offset
            u2["clip"] = k
            utterances.append(u2)
        if part.get("text"):
            texts.append(part["text"])
        clips_meta.append({"index": k,
                           "duration_sec": round(float(dur), 3),
                           "words": len(part["words"]),
                           "request_id": part.get("request_id")})
        offset += dur
    first = parts[0] if parts else {}
    return {
        "words": words,
        "utterances": utterances,
        "text": " ".join(texts),
        "provider": "deepgram",
        "model": first.get("model") or DEEPGRAM_MODEL,
        "language": first.get("language") or "en",
        "duration": offset,
        "request_id": first.get("request_id"),
        "transcribed_at": datetime.now(timezone.utc).isoformat(),
        "multiclip": True,
        "clips": clips_meta,
    }


def transcribe_job_sources(job, src_paths, durations, tmp):
    """Multi-clip transcription: Deepgram per source file, stitched
    into one stream (stitch_transcripts). A one-source job delegates
    to transcribe_job unchanged, so the single-clip path (stored
    shape, progress, messages) is byte-for-byte the C3 behaviour.
    Idempotent like C3: an existing transcript is never redone. A
    clip with no audible speech fails the job honestly - silently
    dropping a silent middle clip would silently drop part of the
    take the creator filmed."""
    jid = job["id"]
    if job.get("transcript_words"):
        return job["transcript_words"]
    if len(src_paths) == 1:
        return transcribe_job(job, src_paths[0], tmp)
    rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
         {"status": "transcribing", "progress": 5, "stage": "transcribing"})
    parts = []
    for k, src in enumerate(src_paths):
        audio = os.path.join(tmp, f"audio{k}.wav")
        extract_audio(src, audio)
        result = deepgram_transcribe(audio)
        if not result["words"]:
            raise TranscriptionError(
                f"We couldn't hear any speech in clip {k + 1} of this "
                "video, so there's nothing to transcribe in it. "
                "Check that clip's audio, or remove the clip and "
                "try again.")
        parts.append(result)
        set_progress(jid, 5 + int(9 * (k + 1) / len(src_paths)),
                     "transcribing")
    stitched = stitch_transcripts(parts, durations)
    rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
         {"transcript_words": stitched, "progress": 14,
          "stage": "transcribed"})
    print(f"job {jid}: transcribed {len(stitched['words'])} words "
          f"across {len(src_paths)} clips ({stitched['model']})")
    return stitched


# ---------------------------------------------------------------------------
# C4: edit-decision pass (rules + Claude -> EDL).
#
# Consumes the stored transcript (C3) plus the measured silences from the
# tight silencedetect recipe, and persists the job's `edl`: ordered
# keep/cut ranges tiling [0, duration] exactly, every cut carrying a
# reason from the spec vocabulary (speech | silence | filler | retake |
# off_topic | duplicate_point) and a short human note - no silent edits.
#
# HARD RULE - no fabricated timestamps: the LLM never sees or returns
# times. Claude returns WORD INDEX ranges only; every EDL boundary is
# derived from Deepgram word start/end times or measured silence edges
# (asserted by validate_edl before anything is stored). Deterministic
# rules run first (fillers from the transcript, silences confirmed by
# BOTH a word gap and a silencedetect span); Claude judges only the
# semantic reasons (retake / off_topic / duplicate_point).
#
# Failure behavior: any failure of the pass (missing key, API error,
# unparseable or contract-breaking model output, EDL validation) marks
# llm_pass='failed' and raises EditError; process() fails the job. There
# is NO silent fallback to a silence-only EDL. An existing EDL is never
# overwritten: once stored it is the job's decision record (a re-decision
# is a new job), so a re-queued job skips this pass entirely.
#
# Prompts and transcripts are job data: they are never written to
# cut_events/analytics and never used for training; worker logs carry
# counts and reason tallies only.
# ---------------------------------------------------------------------------

EDIT_MODEL = os.environ.get("CUT_EDIT_MODEL", "claude-sonnet-4-6")
FILLER_WORDS = {"um", "uh", "umm", "uhh", "er", "erm", "hmm", "mm", "mhm"}
SILENCE_MIN_GAP = 0.40  # matches the tight recipe's silencedetect d=0.4
SEMANTIC_REASONS = ("retake", "off_topic", "duplicate_point")
REASON_PRIORITY = {"retake": 3, "off_topic": 3, "duplicate_point": 3,
                   "filler": 2, "silence": 1}


class EditError(Exception):
    """An edit-pass failure whose message is safe to show the user."""


def anthropic_key():
    """Resolve the Anthropic API key (C1-PLAN item (h) plumbing); the
    value is never logged or stored anywhere but the auth header. Order:
    process environment (the Render service env), then the worker env
    file (.env.local carries the key app-side)."""
    k = os.environ.get("ANTHROPIC_API_KEY") or ENV.get("ANTHROPIC_API_KEY")
    return k.strip() if k else None


def _norm_token(w):
    return re.sub(r"[^a-z0-9]", "", (w or "").lower())


def build_edit_prompt(words, utterances, duration, boundaries=None):
    """Number the transcript words (grouped by utterance) for Claude.
    Indices are the only positional language the model is given.
    `boundaries` (Multi-Clip Jobs) lists the word indices at which a
    new source clip starts - the camera restarts. The model is told
    where they are (as indices, never times) because a restart is a
    retake signal: creators stop the camera mid-line and re-take the
    line when it starts again. With no boundaries the prompt is the
    C4 prompt byte-for-byte."""
    keys = []
    for w in words:
        k = -1
        for ui, u in enumerate(utterances or []):
            if w["start"] >= u["start"] - 0.05 and w["end"] <= u["end"] + 0.05:
                k = ui
                break
        keys.append(k)
    lines = []
    cur = 0
    for i in range(1, len(words) + 1):
        if i == len(words) or keys[i] != keys[cur]:
            toks = " ".join(
                f"({j}){words[j].get('punctuated_word') or words[j]['word']}"
                for j in range(cur, i))
            lines.append(f"Words {cur}-{i - 1}: {toks}")
            cur = i
    body = "\n".join(lines)
    boundary_note = ""
    if boundaries:
        starts = ", ".join(f"word {b}" for b in boundaries)
        boundary_note = (
            "\n\nThis take was filmed as "
            f"{len(boundaries) + 1} separate clips joined in order. "
            "The camera stopped and restarted right before: "
            + starts + ". A camera restart almost always means the "
            "speaker is re-taking the line they were on: when an "
            "attempt runs into a restart and the line is tried again "
            "in the next clip, cut the earlier attempt as a retake "
            "and keep the later complete one, exactly as you would "
            "for a retake inside one clip.")
    return (
        "You are the edit-decision pass for Hook Lab Cut, a tool that "
        "trims creator talking-head videos. Below is the word-level "
        f"transcript of one video ({duration:.2f} seconds, {len(words)} "
        "words). Every word is numbered; the numbers are the ONLY way to "
        "refer to positions - never use seconds or timestamps.\n\n"
        + body + boundary_note +
        "\n\nIdentify ranges of words to CUT for exactly these reasons:\n"
        "- \"retake\": an earlier attempt at a line that the speaker tries "
        "again later. Cut the earlier failed or incomplete attempts and "
        "KEEP the last complete attempt. The note must say which take was "
        "kept, e.g. \"retake - kept take 3\".\n"
        "- \"off_topic\": a sentence or passage unrelated to the video's "
        "main subject (judge the subject from the bulk of the "
        "transcript).\n"
        "- \"duplicate_point\": the same point made more than once. Cut "
        "the weaker instance, keep the stronger one; the note must say "
        "which instance was kept.\n\n"
        "Rules: only return ranges you are confident about - when in "
        "doubt, KEEP the words. Never cut the only complete statement of "
        "the main point, and never cut a closing call to action that "
        "appears once. Do not mark silences or single filler words "
        "(um/uh) - those are handled separately. Ranges must be in "
        "ascending order and must not overlap.\n\n"
        "Respond with ONLY a JSON object, no other text:\n"
        "{\"cuts\": [{\"start_word\": <int>, \"end_word\": <int>, "
        "\"reason\": \"retake\"|\"off_topic\"|\"duplicate_point\", "
        "\"note\": \"<short human-readable note>\"}]}\n"
        "If nothing should be cut, return {\"cuts\": []}.")


def _call_claude(prompt):
    """One Anthropic Messages call via curl (the worker's egress pattern;
    urllib reads are truncated by the proxy). Returns the response text.
    Raises EditError with a plain-language message on any failure."""
    key = anthropic_key()
    if not key:
        raise EditError(
            "The editing service isn't configured on the worker yet "
            "(the Anthropic API key is missing). Please try again later.")
    payload = {
        "model": EDIT_MODEL,
        "max_tokens": 8192,
        "temperature": 0,
        "messages": [{"role": "user", "content": prompt}],
    }
    fd, pfile = tempfile.mkstemp(suffix=".json")
    rfile = pfile + ".resp"
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f)
        p = subprocess.run(
            ["curl", "-s", "-o", rfile, "-w", "%{http_code}",
             "--connect-timeout", "15", "--max-time", "300", "-X", "POST",
             "https://api.anthropic.com/v1/messages",
             "-H", f"x-api-key: {key}",
             "-H", "anthropic-version: 2023-06-01",
             "-H", "Content-Type: application/json",
             "--data-binary", "@" + pfile],
            capture_output=True, text=True)
        code = (p.stdout or "").strip()
        if code != "200":
            detail = ""
            try:
                with open(rfile) as f:
                    detail = ((json.load(f).get("error") or {})
                              .get("message") or "")[:120]
            except Exception:  # noqa: BLE001 - error body is best-effort
                pass
            if code in ("401", "403"):
                raise EditError(
                    "The editing service rejected the worker's API key. "
                    "Please try again later.")
            if code == "429":
                raise EditError(
                    "The editing service is busy right now. "
                    "Please try again in a few minutes.")
            raise EditError(
                "The editing service returned an error "
                f"({code or 'no response'}"
                + (f": {detail}" if detail else "")
                + "). Please try again.")
        with open(rfile) as f:
            resp = json.load(f)
        note_claude_usage(resp.get("usage"))
        texts = [b.get("text", "") for b in (resp.get("content") or [])
                 if b.get("type") == "text"]
        if not texts:
            raise EditError(
                "The editing service returned an empty response. "
                "Please try again.")
        return "".join(texts)
    finally:
        for x in (pfile, rfile):
            try:
                os.unlink(x)
            except OSError:
                pass


def claude_semantic_cuts(words, utterances, duration, boundaries=None):
    """Ask Claude for the semantic cut decisions as WORD INDEX ranges
    and validate them against the contract: integral indices inside the
    transcript (clamped/repaired only as index arithmetic - never as
    times), reason inside the semantic vocabulary, notes present.
    Overlapping ranges are merged (reason of the range covering more
    words wins). A response that breaks the contract, or that would cut
    (nearly) the whole video, is rejected wholesale with EditError -
    never partially applied."""
    if not words:
        return []
    text = _call_claude(build_edit_prompt(words, utterances, duration,
                                          boundaries))
    t = (text or "").strip()
    s, e = t.find("{"), t.rfind("}")
    if s < 0 or e <= s:
        raise EditError(
            "The editing service returned a response we couldn't "
            "understand, so nothing was changed. Please try again.")
    try:
        obj = json.loads(t[s:e + 1])
    except Exception:  # noqa: BLE001 - any parse failure is the same failure
        raise EditError(
            "The editing service returned a response we couldn't "
            "understand, so nothing was changed. Please try again.")
    raw = obj.get("cuts") if isinstance(obj, dict) else None
    if not isinstance(raw, list):
        raise EditError(
            "The editing service returned a response we couldn't "
            "understand, so nothing was changed. Please try again.")
    n = len(words)
    cuts = []
    for c in raw:
        if not isinstance(c, dict):
            raise EditError(
                "The editing service returned a cut in an unexpected "
                "shape, so nothing was changed. Please try again.")
        sw, ew = c.get("start_word"), c.get("end_word")
        reason = c.get("reason")
        note = c.get("note")
        if (not isinstance(sw, (int, float)) or isinstance(sw, bool)
                or not isinstance(ew, (int, float)) or isinstance(ew, bool)
                or int(sw) != sw or int(ew) != ew):
            raise EditError(
                "The editing service returned a cut with invalid word "
                "positions, so nothing was changed. Please try again.")
        if reason not in SEMANTIC_REASONS:
            raise EditError(
                "The editing service returned a cut with an unknown "
                "reason, so nothing was changed. Please try again.")
        sw = max(0, min(n - 1, int(sw)))
        ew = max(0, min(n - 1, int(ew)))
        if sw > ew:
            sw, ew = ew, sw
        if not isinstance(note, str) or not note.strip():
            note = f"Flagged as {reason.replace('_', ' ')} by the edit pass."
        cuts.append({"start_word": sw, "end_word": ew, "reason": reason,
                     "note": note.strip()[:200]})
    total = sum(c["end_word"] - c["start_word"] + 1 for c in cuts)
    if total > 0.9 * n:
        raise EditError(
            "The editing service suggested cutting almost the entire "
            "video, so nothing was changed. Please try again.")
    cuts.sort(key=lambda c: (c["start_word"], c["end_word"]))
    merged = []
    for c in cuts:
        if merged and c["start_word"] <= merged[-1]["end_word"]:
            m = merged[-1]
            if (c["end_word"] - c["start_word"]
                    > m["end_word"] - m["start_word"]):
                m["reason"], m["note"] = c["reason"], c["note"]
            m["end_word"] = max(m["end_word"], c["end_word"])
        else:
            merged.append(dict(c))
    return merged


def rule_cuts(words, silences, duration):
    """Deterministic cuts from measurements only - no model involved.
    - filler: each filler word's own [start, end] (nova-2 returns
      um/uh as words because C3 sets filler_words=true).
    - silence: a gap between consecutive words (or before the first /
      after the last word) of at least SILENCE_MIN_GAP that a measured
      silencedetect span overlaps. The cut spans exactly gap edge to
      gap edge, i.e. word times - the measured span only confirms the
      gap is truly silent, it never sets the boundary itself."""
    cuts = []
    for w in words:
        if _norm_token(w.get("word")) in FILLER_WORDS:
            cuts.append({
                "start": w["start"], "end": w["end"], "reason": "filler",
                "note": "filler - removed '"
                        + (w.get("word") or "").strip().lower() + "'"})
    spans = [(ds, de if de is not None else duration) for ds, de in silences]

    def confirmed(gs, ge):
        return any(min(de, ge) - max(ds, gs) > 0.05 for ds, de in spans)

    gaps = [(0.0, words[0]["start"])]
    gaps += [(words[i]["end"], words[i + 1]["start"])
             for i in range(len(words) - 1)]
    gaps.append((words[-1]["end"], duration))
    for gs, ge in gaps:
        if ge - gs >= SILENCE_MIN_GAP and confirmed(gs, ge):
            cuts.append({
                "start": gs, "end": ge, "reason": "silence",
                "note": f"silence - {ge - gs:.1f}s pause"})
    return cuts


def build_edl(words, silences, duration, semantic_cuts):
    """Combine rule cuts + validated semantic cuts into the EDL.
    Semantic cuts arrive as word-index ranges and convert to times ONLY
    through the words' measured start/end. Overlapping or touching cuts
    union-merge; the merged range keeps the highest-priority reason
    (semantic > filler > silence) and its note. Keeps are the complement,
    reason 'speech'. All boundaries are rounded to milliseconds and the
    segments are built sequentially, so the EDL tiles [0, duration]
    exactly with no gaps and no overlaps."""
    dur = round(duration, 3)
    cuts = rule_cuts(words, silences, duration)
    for c in semantic_cuts:
        cuts.append({
            "start": words[c["start_word"]]["start"],
            "end": words[c["end_word"]]["end"],
            "reason": c["reason"], "note": c["note"]})
    for c in cuts:
        c["start"] = round(c["start"], 3)
        c["end"] = round(c["end"], 3)
    cuts = [c for c in cuts if c["end"] - c["start"] >= 0.001]
    cuts.sort(key=lambda c: (c["start"], c["end"]))
    merged = []
    for c in cuts:
        if merged and c["start"] <= merged[-1]["end"] + 0.0005:
            m = merged[-1]
            if REASON_PRIORITY[c["reason"]] > REASON_PRIORITY[m["reason"]]:
                m["reason"], m["note"] = c["reason"], c["note"]
            m["end"] = max(m["end"], c["end"])
        else:
            merged.append(dict(c))
    edl = []
    cur = 0.0
    for c in merged:
        if c["start"] > cur:
            edl.append({"start": cur, "end": c["start"], "action": "keep",
                        "reason": "speech", "note": "Kept speech."})
        edl.append({"start": c["start"], "end": c["end"], "action": "cut",
                    "reason": c["reason"], "note": c["note"]})
        cur = c["end"]
    if cur < dur:
        edl.append({"start": cur, "end": dur, "action": "keep",
                    "reason": "speech", "note": "Kept speech."})
    return edl


def split_edl_at_boundaries(edl, clip_durations):
    """Multi-Clip Jobs: no EDL range may span a camera restart - the
    render reads each range from ONE source file. Split every range
    at the clip boundary offsets (cumulative sums of the measured
    clip durations) and tag each piece with its source clip index.
    Tiling is preserved exactly: the pieces are the range itself, cut
    at the offsets inside it, with action/reason/note copied."""
    bounds = clip_offsets(clip_durations)[1:]
    bounds = [round(b, 3) for b in bounds]
    out = []
    for seg in edl:
        points = [seg["start"]]
        points += [b for b in bounds if seg["start"] < b < seg["end"]]
        points.append(seg["end"])
        for a, b in zip(points, points[1:]):
            piece = dict(seg)
            piece["start"], piece["end"] = a, b
            piece["clip"] = sum(1 for x in bounds if x <= a + 0.0005)
            out.append(piece)
    return out


def validate_edl(edl, words, silences, duration, clip_offsets_list=None):
    """Assert the EDL contract before it is stored: exact tiling of
    [0, duration], ascending, no overlaps, every boundary aligned to a
    transcript word edge, a measured silence edge, 0, or duration
    (within 11 ms - boundaries are stored at millisecond precision).
    Multi-Clip Jobs adds the clip boundary offsets as legal edges:
    they are sums of measured clip durations, i.e. arithmetic on
    measurements, the same class of derivation as every other edge.
    Raises EditError; a failing EDL is never stored."""
    dur = round(duration, 3)
    if not edl:
        raise EditError("The edit pass produced no edit decision list.")
    edges = {0.0, dur}
    for w in words:
        edges.add(round(w["start"], 3))
        edges.add(round(w["end"], 3))
    for ds, de in silences:
        edges.add(round(ds, 3))
        if de is not None:
            edges.add(round(de, 3))
    for o in clip_offsets_list or []:
        edges.add(round(o, 3))
    tol = 0.011
    prev = 0.0
    for seg in edl:
        if abs(seg["start"] - prev) > 0.0005:
            raise EditError(
                "The edit decision list doesn't line up end to end, so "
                "nothing was changed. Please try again.")
        if not seg["end"] > seg["start"]:
            raise EditError(
                "The edit decision list contains an empty range, so "
                "nothing was changed. Please try again.")
        for b in (seg["start"], seg["end"]):
            if min(abs(b - e) for e in edges) > tol:
                raise EditError(
                    "The edit decision list contains a time that doesn't "
                    "match the transcript, so nothing was changed. "
                    "Please try again.")
        if seg["action"] == "cut" and seg["reason"] == "speech":
            raise EditError(
                "The edit decision list contains a cut marked as "
                "speech, so nothing was changed. Please try again.")
        prev = seg["end"]
    if abs(prev - dur) > 0.0005:
        raise EditError(
            "The edit decision list doesn't cover the whole video, so "
            "nothing was changed. Please try again.")


def edit_job(job, transcript, silences, duration):
    """C4 stage: build and persist the job's EDL (rules + Claude) and
    set llm_pass. A job that already has an EDL is never re-decided -
    the stored EDL is final for the job. On any EditError, llm_pass is
    set to 'failed' and the error propagates so process() fails the
    job: it never renders from a partial or invented EDL."""
    jid = job["id"]
    if job.get("edl"):
        print(f"job {jid}: edl already stored - skipping edit pass")
        return job["edl"]
    words = (transcript or {}).get("words") or []
    if not words:
        rest("PATCH", f"hookcut_jobs?id=eq.{jid}", {"llm_pass": "failed"})
        raise EditError(
            "There's no transcript for this video yet, so it can't be "
            "edited. Please try again.")
    set_progress(jid, 29, "editing")
    try:
        # Multi-Clip Jobs: the camera restarts, as word indices (the
        # first word of each clip after the first), go to the judge
        # as a retake signal; the finished EDL is then split at the
        # boundaries so no range spans two source files, and each
        # range is tagged with its source clip. One-clip jobs take
        # none of these branches and store the C4 EDL byte-for-byte.
        boundaries = None
        clip_durations = None
        if (transcript or {}).get("clips"):
            clip_durations = [c["duration_sec"]
                              for c in transcript["clips"]]
            tags = [w.get("clip") for w in words]
            if any(t is not None for t in tags):
                boundaries = [i for i in range(1, len(words))
                              if tags[i] != tags[i - 1]]
        semantic = claude_semantic_cuts(
            words, (transcript or {}).get("utterances"), duration,
            boundaries)
        edl = build_edl(words, silences, duration, semantic)
        offsets = None
        if clip_durations:
            edl = split_edl_at_boundaries(edl, clip_durations)
            offsets = clip_offsets(clip_durations)
        validate_edl(edl, words, silences, duration, offsets)
    except EditError:
        rest("PATCH", f"hookcut_jobs?id=eq.{jid}", {"llm_pass": "failed"})
        raise
    rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
         {"edl": edl, "llm_pass": "ok"})
    n_cut = sum(1 for s in edl if s["action"] == "cut")
    kept = sum(s["end"] - s["start"] for s in edl if s["action"] == "keep")
    by_reason = {}
    for s in edl:
        if s["action"] == "cut":
            by_reason[s["reason"]] = by_reason.get(s["reason"], 0) + 1
    print(f"job {jid}: edl stored - {len(edl)} segments, {n_cut} cuts "
          f"{by_reason}, kept {kept:.2f}s of {duration:.2f}s, llm_pass=ok")
    return edl




# ---------------------------------------------------------------------------
# C5: render from the persisted EDL (replaces the prototype render).
#
# process() now renders the job from its stored EDL (C4): ONLY the keep
# ranges are concatenated, in order. The old silence-driven render
# (build_segments + render) is no longer called for any job - every job
# that reaches this stage has an EDL (edit_job returns one or raises).
#
# Joins (C5 default): a 30 ms crossfade at each join - audio acrossfade
# and video xfade (transition=fade) - so BOTH streams shrink by the
# same amount per faded join and stay in sync. The video join is a
# short dissolve between the two shots: no black frames and no frozen
# frames are inserted. A join is faded only when BOTH neighbouring
# keeps are at least FADE_MIN_SEG_SEC long (a fade must never overrun
# a segment); a shorter neighbour hard-joins (plain concat) on both
# streams, using the same per-join decision for audio and video so
# sync is preserved.
# Two measured xfade requirements shape the implementation:
# - Both inputs must share one timebase: every segment and every
#   xfade output is normalized with settb=1/fps. Without that,
#   chained xfades silently truncate the video (Test Take A: 35.3 s
#   of audio with only 9.5 s of video).
# - The fade offsets must be frame-exact. Millisecond-rounded EDL
#   times drift against the frame-quantized stream, and the first
#   offset whose offset+duration overshoots the accumulated stream
#   truncates the chain at that join. fade_plan therefore works in
#   whole frames: each keep's frame count is counted exactly from
#   the source's frame timestamps (the frames trim will pass are
#   precisely those with pts in [start, end)), the fade is
#   round(30 ms * fps) whole frames (1 frame at 30 fps = 33.3 ms -
#   the closest a frame grid gets to 30 ms), offsets are placed so
#   each fade ends exactly on the accumulated frame count, and that
#   same fade duration drives acrossfade and the caption retiming,
#   so audio, video, and captions shrink identically. After the
#   render, process() verifies the output's frame count against
#   the plan and fails the job rather than shipping a truncated
#   video if they disagree.
#
# Loudness (C5 spec value): loudnorm to -14 LUFS (TP -1.5, LRA 11 as
# in the prototype recipe; the prototype's -16 target is superseded).
# ffmpeg's loudnorm under-applies gain in dynamic mode on peaky
# speech - measured on Test Take A's joined audio, one textbook
# two-pass application landed at -15.2 LUFS against the -14 target -
# so build_output_audio measures the joined audio, applies loudnorm
# with the measured stats, re-measures, and re-applies with fresh
# stats until within LOUDNORM_TOL of the target (cap
# LOUDNORM_MAX_PASSES). The final measured value is logged per job.
#
# Captions: cues are built from the stored transcript words of the
# KEPT segments only and retimed onto the output timeline (source
# word time minus the accumulated cut/fade offsets) - words from cut
# segments can never appear. The same cues are (a) burned into the
# video in the C5 default style (bold white text, black outline,
# bottom-center, via an ASS file + the ass filter) and (b) written as
# an SRT uploaded to the job's captions_srt_key. Grouping: at most
# CAPTION_MAX_WORDS words per cue, breaking at utterance boundaries
# where the transcript provides them, at output-timeline gaps over
# CAPTION_GAP_SEC, and past CAPTION_MAX_DUR seconds per cue.
# ---------------------------------------------------------------------------

CROSSFADE_SEC = 0.030  # C5 default join crossfade: 30 ms
FADE_MIN_SEG_SEC = 0.12  # both neighbours >= 4x the fade to fade a join
LOUDNORM_I = -14.0  # C5 loudness target (LUFS)
LOUDNORM_TP = -1.5
LOUDNORM_LRA = 11
LOUDNORM_TOL = 0.3  # LUFS - inside this, stop re-applying loudnorm
LOUDNORM_MAX_PASSES = 3
CAPTION_MAX_WORDS = 4
CAPTION_MAX_DUR = 4.0
CAPTION_GAP_SEC = 0.6
CAPTION_FONT = "DejaVu Sans"
# Timeline Simplification (Matt's review, Oct 26 2026): no burned
# subtitles unless someone asks for them. The initial render and
# exports default to CAPTIONS_DEFAULT ("srt": caption cues are
# still built and uploaded as the downloadable SRT sidecar; nothing
# is burned into the picture). Burning happens only for an export
# whose settings carry captions="burned" (the editor's "Burn
# captions into the video" toggle). The caption pipeline itself -
# cues, SRT, karaoke derivation - is unchanged.
CAPTIONS_DEFAULT = "srt"

STALL_SECONDS = 120


class StallError(RuntimeError):
    """ffmpeg produced no progress output for STALL_SECONDS: it is wedged
    (classically on an inherited stdin, or a starved host). Raised so the
    job can be requeued instead of freezing the progress bar forever."""



# ---------------------------------------------------------------------------
# C5: render from the persisted EDL (replaces the prototype render).
#
# process() now renders the job from its stored EDL (C4): ONLY the keep
# ranges are concatenated, in order. The old silence-driven render
# (build_segments + render) is no longer called for any job - every job
# that reaches this stage has an EDL (edit_job returns one or raises).
#
# Joins (C5 default): a 30 ms crossfade at each join - audio acrossfade
# and video xfade (transition=fade) - so BOTH streams shrink by the
# same amount per faded join and stay in sync. The video join is a
# short dissolve between the two shots: no black frames and no frozen
# frames are inserted. A join is faded only when BOTH neighbouring
# keeps are at least FADE_MIN_SEG_SEC long (a fade must never overrun
# a segment); a shorter neighbour hard-joins (plain concat) on both
# streams, using the same per-join decision for audio and video so
# sync is preserved.
# Two measured xfade requirements shape the implementation:
# - Both inputs must share one timebase: every segment and every
#   xfade output is normalized with settb=1/fps. Without that,
#   chained xfades silently truncate the video (Test Take A: 35.3 s
#   of audio with only 9.5 s of video).
# - The fade offsets must be frame-exact. Millisecond-rounded EDL
#   times drift against the frame-quantized stream, and the first
#   offset whose offset+duration overshoots the accumulated stream
#   truncates the chain at that join. fade_plan therefore works in
#   whole frames: each keep's frame count is counted exactly from
#   the source's frame timestamps (the frames trim will pass are
#   precisely those with pts in [start, end)), the fade is
#   round(30 ms * fps) whole frames (1 frame at 30 fps = 33.3 ms -
#   the closest a frame grid gets to 30 ms), offsets are placed so
#   each fade ends exactly on the accumulated frame count, and that
#   same fade duration drives acrossfade and the caption retiming,
#   so audio, video, and captions shrink identically. After the
#   render, process() verifies the output's frame count against
#   the plan and fails the job rather than shipping a truncated
#   video if they disagree.
#
# Loudness (C5 spec value): loudnorm to -14 LUFS (TP -1.5, LRA 11 as
# in the prototype recipe; the prototype's -16 target is superseded).
# ffmpeg's loudnorm under-applies gain in dynamic mode on peaky
# speech - measured on Test Take A's joined audio, one textbook
# two-pass application landed at -15.2 LUFS against the -14 target -
# so build_output_audio measures the joined audio, applies loudnorm
# with the measured stats, re-measures, and re-applies with fresh
# stats until within LOUDNORM_TOL of the target (cap
# LOUDNORM_MAX_PASSES). The final measured value is logged per job.
#
# Captions: cues are built from the stored transcript words of the
# KEPT segments only and retimed onto the output timeline (source
# word time minus the accumulated cut/fade offsets) - words from cut
# segments can never appear. The same cues are (a) burned into the
# video in the C5 default style (bold white text, black outline,
# bottom-center, via an ASS file + the ass filter) and (b) written as
# an SRT uploaded to the job's captions_srt_key. Grouping: at most
# CAPTION_MAX_WORDS words per cue, breaking at utterance boundaries
# where the transcript provides them, at output-timeline gaps over
# CAPTION_GAP_SEC, and past CAPTION_MAX_DUR seconds per cue.
# ---------------------------------------------------------------------------

CROSSFADE_SEC = 0.030  # C5 default join crossfade: 30 ms
FADE_MIN_SEG_SEC = 0.12  # both neighbours >= 4x the fade to fade a join
LOUDNORM_I = -14.0  # C5 loudness target (LUFS)
LOUDNORM_TP = -1.5
LOUDNORM_LRA = 11
LOUDNORM_TOL = 0.3  # LUFS - inside this, stop re-applying loudnorm
LOUDNORM_MAX_PASSES = 3
CAPTION_MAX_WORDS = 4
CAPTION_MAX_DUR = 4.0
CAPTION_GAP_SEC = 0.6
CAPTION_FONT = "DejaVu Sans"


class RenderError(RuntimeError):
    """A render failure whose message is safe to show the user."""


def probe_video(path):
    """(width, height, settb string, fps_num, fps_den) for the first
    video stream. The xfade timebase is 1/fps as an exact rational
    (avg_frame_rate num/den -> settb den/num); falls back to AVTB and
    a 0 rate when the rate is unreadable."""
    out = subprocess.check_output(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=width,height,avg_frame_rate",
         "-of", "json", path], text=True)
    s = (json.loads(out).get("streams") or [{}])[0]
    w, h = int(s.get("width") or 0), int(s.get("height") or 0)
    tb = "AVTB"
    num, den = 0, 1
    try:
        n, d = (s.get("avg_frame_rate") or "0/0").split("/")
        num, den = int(n), int(d)
        if num > 0 and den > 0:
            tb = f"{den}/{num}"
        else:
            num, den = 0, 1
    except Exception:  # noqa: BLE001 - any parse failure keeps AVTB
        num, den = 0, 1
    return w, h, tb, num, den



def output_dims(w, h):
    """Dimensions of the rendered output (and of the caption canvas):
    the render host's existing 1080x1920 cap, never upscaling (the
    same rule render() applies per segment)."""
    if w <= 1080 and h <= 1920:
        return w, h
    f = min(1080 / w, 1920 / h)
    return max(2, round(w * f) // 2 * 2), max(2, round(h * f) // 2 * 2)



def probe_frame_times(path):
    """Presentation timestamps (seconds, ascending) of every video
    frame, read from packet headers (no decoding). The frames an
    EDL keep contains are exactly those with pts in [start, end) -
    the same set trim passes - so counting them from this list is
    exact for a constant-rate source."""
    out = subprocess.check_output(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "packet=pts_time", "-of", "csv=p=0", path],
        text=True)
    times = []
    for line in out.splitlines():
        line = line.strip().strip(",")
        if line:
            try:
                times.append(float(line))
            except ValueError:
                pass
    times.sort()
    return times


def fade_plan(keeps, fps_num, fps_den, frame_times=None):
    """The join plan, computed in whole frames (see the C5 block
    comment for why): (fades, fade_sec, seg_frames). seg_frames[i]
    is the exact number of source frames in keeps[i], counted from
    frame_times when available, else estimated from the keep's
    duration. fades[i] joins keeps[i] and keeps[i+1] and is True
    only when the frame rate is known, both keeps are at least
    FADE_MIN_SEG_SEC long, and both hold more frames than the fade.
    fade_sec is the frame-quantized crossfade duration
    (round(30 ms * fps) frames, at least 1 frame at any known rate;
    exactly CROSSFADE_SEC when the rate is unknown - in that case
    seg_frames is all zeros, no join fades, and the render simply
    concatenates, since chained xfades cannot be trusted without a
    frame rate). One plan drives the video chain, the audio chain,
    and the caption retiming, so all three agree exactly."""
    known = fps_num > 0 and fps_den > 0
    if known:
        fps = fps_num / fps_den
        fade_frames = max(1, int(CROSSFADE_SEC * fps + 0.5))
        fade_sec = fade_frames * fps_den / fps_num
        if frame_times:
            import bisect
            seg_frames = [
                bisect.bisect_left(frame_times, b)
                - bisect.bisect_left(frame_times, a)
                for a, b in keeps]
        else:
            seg_frames = [max(1, int((b - a) * fps + 0.5))
                          for a, b in keeps]
    else:
        fade_frames, fade_sec = 0, CROSSFADE_SEC
        seg_frames = [0] * len(keeps)
    fades = []
    for i in range(len(keeps) - 1):
        ok = (known
              and keeps[i][1] - keeps[i][0] >= FADE_MIN_SEG_SEC
              and keeps[i + 1][1] - keeps[i + 1][0] >= FADE_MIN_SEG_SEC
              and seg_frames[i] > fade_frames
              and seg_frames[i + 1] > fade_frames)
        fades.append(bool(ok))
    return fades, fade_sec, seg_frames


def fade_frames_from(fade_sec, fps_num, fps_den):
    if fps_num > 0 and fps_den > 0:
        return int(round(fade_sec * fps_num / fps_den))
    return 0


def keep_offsets(keeps, fades, fade_sec):
    """Output-timeline start of each keep: the accumulated kept
    durations before it, minus one fade duration for each faded join
    before it."""
    offs = [0.0]
    for i in range(1, len(keeps)):
        offs.append(offs[-1] + (keeps[i - 1][1] - keeps[i - 1][0])
                    - (fade_sec if fades[i - 1] else 0.0))
    return offs


def _utt_index(word, utterances):
    for ui, u in enumerate(utterances or []):
        if word["start"] >= u["start"] - 0.05 and word["end"] <= u["end"] + 0.05:
            return ui
    return -1


def _cue_text(words):
    text = " ".join(
        (w.get("punctuated_word") or w.get("word") or "") for w in words)
    text = re.sub(r"\s+([,.!?;:%])", r"\1", text)
    return re.sub(r"\s+", " ", text).strip()



def count_output_frames(path):
    """Frame count of a rendered file from its stream metadata
    (None when the container doesn't carry one)."""
    try:
        out = subprocess.check_output(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=nb_frames", "-of", "csv=p=0",
             path], text=True).strip().strip(",")
        return int(out)
    except Exception:  # noqa: BLE001 - verification is best-effort
        return None

def build_captions(transcript, keeps, fades, fade_sec):
    """Caption cues on the OUTPUT timeline, from the stored transcript
    words that fall inside kept EDL segments only. Each kept word's
    output time is its source time shifted by its keep's offset (which
    already accounts for every cut and fade before it). Words whose
    midpoint falls in a cut segment are excluded - no retake, weather,
    or cut filler word can reach a caption."""
    words = (transcript or {}).get("words") or []
    utterances = (transcript or {}).get("utterances") or []
    offs = keep_offsets(keeps, fades, fade_sec)
    total = (offs[-1] + (keeps[-1][1] - keeps[-1][0])) if keeps else 0.0
    kept = []
    for w in words:
        mid = (w["start"] + w["end"]) / 2.0
        for ki, (a, b) in enumerate(keeps):
            if a - 0.001 <= mid <= b + 0.001:
                kept.append((offs[ki] + (w["start"] - a),
                             offs[ki] + (w["end"] - a), w,
                             _utt_index(w, utterances)))
                break
    # C6: keeps may be reordered by data (the best-hook toggle
    # moves a keep to the front of the EDL), so order the kept
    # words by their OUTPUT time before grouping cues. For an
    # ascending EDL the words already arrive in that order -
    # this sort is a no-op there.
    kept.sort(key=lambda it: (it[0], it[1]))
    cues = []
    cur = []

    def flush():
        if cur:
            cues.append({
                "start": round(cur[0][0], 3),
                "end": round(min(cur[-1][1], total), 3),
                "text": _cue_text([c[2] for c in cur])})

    prev = None
    for item in kept:
        if cur:
            gap = item[0] - prev[1]
            utt_break = (item[3] != prev[3] and item[3] != -1
                         and prev[3] != -1)
            if (utt_break or len(cur) >= CAPTION_MAX_WORDS
                    or gap > CAPTION_GAP_SEC
                    or (item[1] - cur[0][0]) > CAPTION_MAX_DUR):
                flush()
                cur = []
        cur.append(item)
        prev = item
    flush()
    return [c for c in cues if c["text"] and c["end"] > c["start"]]


def _srt_ts(sec):
    ms = int(round(sec * 1000))
    h, ms = divmod(ms, 3600000)
    m, ms = divmod(ms, 60000)
    s, ms = divmod(ms, 1000)
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"


def captions_srt(cues):
    blocks = []
    for i, c in enumerate(cues, 1):
        blocks.append(
            f"{i}\n{_srt_ts(c['start'])} --> {_srt_ts(c['end'])}\n"
            f"{c['text']}\n")
    return "\n".join(blocks)


def _ass_ts(sec):
    cs = int(round(sec * 100))
    h, cs = divmod(cs, 360000)
    m, cs = divmod(cs, 6000)
    s, cs = divmod(cs, 100)
    return f"{h}:{m:02d}:{s:02d}.{cs:02d}"


def _ass_text(text):
    """Escape a cue for an ASS Dialogue line, wrapped to two lines
    when long. Braces start override blocks in ASS, so they are
    neutralized; a backslash would start an override tag."""
    text = text.replace("\\", "").replace("{", "(").replace("}", ")")
    if len(text) > 42:
        words = text.split(" ")
        best, acc = 1, 0
        for i, wd in enumerate(words[:-1]):
            acc += len(wd) + 1
            if abs(acc - len(text) / 2) < abs(
                    sum(len(x) + 1 for x in words[:best]) - len(text) / 2):
                best = i + 1
        text = " ".join(words[:best]) + "\\N" + " ".join(words[best:])
    return text


def captions_ass(cues, width, height):
    """The C5 default caption style as an ASS document: bold white
    text, black outline, bottom-center (Alignment 2, BorderStyle 1)."""
    fs = max(28, int(round(height * 0.042)))
    outline = max(2, int(round(height * 0.0023)))
    margin_v = int(round(height * 0.03))
    margin_lr = max(10, int(round(width * 0.055)))
    lines = [
        "[Script Info]",
        "Title: Hook Lab Cut captions",
        "ScriptType: v4.00+",
        f"PlayResX: {width}",
        f"PlayResY: {height}",
        "WrapStyle: 2",
        "ScaledBorderAndShadow: yes",
        "",
        "[V4+ Styles]",
        ("Format: Name, Fontname, Fontsize, PrimaryColour, "
         "SecondaryColour, OutlineColour, BackColour, Bold, Italic, "
         "Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, "
         "BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, "
         "MarginV, Encoding"),
        (f"Style: Default,{CAPTION_FONT},{fs},&H00FFFFFF,&H00FFFFFF,"
         f"&H00000000,&H00000000,1,0,0,0,100,100,0,0,1,{outline},0,2,"
         f"{margin_lr},{margin_lr},{margin_v},1"),
        "",
        "[Events]",
        ("Format: Layer, Start, End, Style, Name, MarginL, MarginR, "
         "MarginV, Effect, Text"),
    ]
    for c in cues:
        lines.append(
            f"Dialogue: 0,{_ass_ts(c['start'])},{_ass_ts(c['end'])},"
            f"Default,,0,0,0,,{_ass_text(c['text'])}")
    return "\n".join(lines) + "\n"


def measure_loudness(path):
    """Integrated-loudness measurement of an audio file with the C5
    target settings; returns loudnorm's stats dict (string values) or
    None when the measurement cannot be parsed."""
    af = (f"loudnorm=I={LOUDNORM_I}:TP={LOUDNORM_TP}:LRA={LOUDNORM_LRA}"
          ":print_format=json")
    p = subprocess.run(
        ["ffmpeg", "-i", path, "-vn", "-af", af, "-f", "null", "-"],
        capture_output=True, text=True)
    m = re.search(r"\{\s*\"input_i\".*?\}", p.stderr, re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except Exception:  # noqa: BLE001 - unparseable stats = no measurement
        return None


def _audio_chain(keeps, fades, fade_sec):
    """The output-timeline audio chain for the keeps: atrim each keep,
    acrossfade at faded joins, plain concat at hard joins (the same
    per-join decisions as the video chain)."""
    parts = []
    for i, (a, b) in enumerate(keeps):
        parts.append(
            f"[0:a]atrim=start={a:.3f}:end={b:.3f},"
            f"asetpts=PTS-STARTPTS[a{i}]")
    cur = "a0"
    for i in range(1, len(keeps)):
        if fades[i - 1]:
            parts.append(
                f"[{cur}][a{i}]acrossfade=d={fade_sec:.6f}"
                f":c1=tri:c2=tri[ax{i}]")
        else:
            parts.append(f"[{cur}][a{i}]concat=n=2:v=0:a=1[ax{i}]")
        cur = f"ax{i}"
    return ";".join(parts), cur


def build_output_audio(src, keeps, fades, tmp, fade_sec):
    """Join the keeps' audio onto the output timeline and normalize
    it to the C5 loudness target. Returns (wav_path, measured_i,
    passes): the final WAV, its measured integrated LUFS (None when
    unmeasurable), and how many loudnorm applications were needed.
    The loop exists because loudnorm's dynamic engine under-applies
    gain on peaky speech (see the C5 block comment): measure, apply
    with the measured stats, re-measure, stop inside the tolerance."""
    graph, cur = _audio_chain(keeps, fades, fade_sec)
    joined = os.path.join(tmp, "joined.wav")
    try:
        p = subprocess.run(
            ["ffmpeg", "-y", "-v", "error", "-i", src,
             "-filter_complex", graph, "-map", f"[{cur}]", joined],
            capture_output=True, text=True, timeout=1800)
    except subprocess.TimeoutExpired:
        raise RenderError(
            "Building the audio for this cut took too long. "
            "Please try again.")
    if p.returncode != 0:
        raise RenderError(
            "We couldn't build the audio for this cut. "
            "Please try again.")
    return normalize_loudness(joined, tmp)


def normalize_loudness(cur_path, tmp, tag=""):
    """The C5 iterative loudnorm loop, shared by the C5 render (via
    build_output_audio) and the E7 timeline export: measure, apply
    loudnorm with the measured stats, re-measure, stop inside the
    tolerance (loudnorm's dynamic engine under-applies gain on peaky
    speech — see the C5 block comment). Returns (wav_path,
    measured_i, passes). `tag` namespaces the temp files when two
    normalizations share a tmp dir."""
    stats = measure_loudness(cur_path)
    passes = 0
    while (stats and abs(float(stats["input_i"]) - LOUDNORM_I) > LOUDNORM_TOL
           and passes < LOUDNORM_MAX_PASSES):
        nxt = os.path.join(tmp, f"norm{tag}{passes}.wav")
        chain = (
            f"loudnorm=I={LOUDNORM_I}:TP={LOUDNORM_TP}:LRA={LOUDNORM_LRA}"
            f":measured_i={stats['input_i']}:measured_tp={stats['input_tp']}"
            f":measured_lra={stats['input_lra']}"
            f":measured_thresh={stats['input_thresh']}"
            f":offset={stats['target_offset']}:linear=false")
        try:
            p = subprocess.run(
                ["ffmpeg", "-y", "-v", "error", "-i", cur_path,
                 "-af", chain, nxt],
                capture_output=True, text=True, timeout=1800)
        except subprocess.TimeoutExpired:
            raise RenderError(
                "Normalizing the audio for this cut took too long. "
                "Please try again.")
        if p.returncode != 0:
            raise RenderError(
                "We couldn't normalize the audio for this cut. "
                "Please try again.")
        cur_path = nxt
        passes += 1
        stats = measure_loudness(cur_path)
    measured = float(stats["input_i"]) if stats else None
    return cur_path, measured, passes



def render_from_edl(src, keeps, fades, ass_path, audio_path, dest, jid,
                    tb, fps_num, fps_den, fade_sec, tmp):
    """C5 render (host build): the EDL keeps joined per the C5
    defaults (frame-exact xfade dissolves at faded joins, hard
    concat otherwise), the caption cues burned in, and the
    pre-normalized output audio from build_output_audio muxed in.
    Each segment keeps the host build's existing 1080x1920 cap (see
    render()).

    Torture-fix F2: the keeps are first rendered as stand-alone
    segment files through the E7 segment chain (trim -> setpts ->
    fps=<source avg rate> conform -> settb, at this build's capped
    dimensions), so every segment is constant-rate BY CONSTRUCTION,
    and the join (join_export_video, the builder the export and
    Multi-Clip paths already use) is planned against the segments'
    MEASURED frame counts. The old single-pass graph planned fades
    from the source's packet timestamps while xfade consumed the
    raw variable-rate stream: on stepped-VFR phone footage the plan
    and the emitted frames desynced (the fail-closed frame check
    failed 4/4) and one irregular Main10 source emitted no packets
    at all and died with raw ffmpeg stderr in the job's error
    field. Planning after normalization removes the class; on
    constant-rate sources the measured counts equal the old packet
    counts exactly, so the golden path does not move. One tier
    deeper: when a source's timestamps are not merely variable but
    scrambled (segment frame counts disagreeing with the keep
    durations in either direction), every segment is re-rendered
    by decode-order index - see the broken-timestamps tier below.
    Without a readable frame rate there is nothing to conform to:
    the legacy single-pass concat graph at the bottom runs instead
    (no fades - fade_plan already decided; the host scale and the
    level 4.2 cap are preserved there). Returns
    (total_seconds, planned_frames); planned_frames is None on the
    legacy path, where no frame plan exists to check against."""
    fps_ok = fps_num > 0 and fps_den > 0
    if fps_ok:
        w, h, _t, _n, _d = probe_video(src)
        ow, oh = export_dims(w, h, "source")
        fps_frac = f"{fps_num}/{fps_den}"
        seg_paths, seg_frames, seg_clips = [], [], []
        for i, (a, b) in enumerate(keeps):
            seg = os.path.join(tmp, f"c5seg{i}.mp4")
            clip = {"source_start": a, "source_end": b,
                    "speed": 1.0, "mute": False, "volume": 1.0,
                    "fade_in_ms": 0, "fade_out_ms": 0}
            seg_clips.append(clip)
            try:
                render_export_segment(src, clip, ow, oh, tb,
                                      fps_frac, seg)
            except ExportError as e:
                print(f"job {jid}: segment render detail: {e}")
                raise RenderError(
                    "We couldn't render this cut. "
                    "Please try again.")
            seg_paths.append(seg)
            seg_frames.append(len(probe_frame_times(seg)))
            set_progress(jid, 30 + int(20 * (i + 1) / len(keeps)),
                         "rendering")
        # Broken-timestamps tier (torture-fix F2): on a healthy
        # source - constant-rate or ordinary variable-rate - every
        # segment's measured frame count agrees with its keep
        # duration at the average rate (ordinary VFR halves sit at
        # ~2/3 and ~4/3 of expectation). A source whose timestamps
        # are scrambled (multi-second pts holes AND clumps: some
        # keeps hold almost no frames, others several times too
        # many) makes timestamp-based segment durations meaningless
        # in BOTH directions, so the whole job is re-rendered by
        # decode-order index, the one ordering that is still
        # content-true, and re-measured.
        def _expected(a, b):
            return (b - a) * fps_num / fps_den
        broken = any(
            abs(n - _expected(a, b)) > max(2.0, 0.5 * _expected(a, b))
            for n, (a, b) in zip(seg_frames, keeps))
        if broken:
            print(f"job {jid}: source timestamps are broken "
                  f"(segment frame counts disagree with the keep "
                  f"durations); re-rendering all segments by "
                  f"decode-order index")
            seg_frames = []
            for i, (a, b) in enumerate(keeps):
                try:
                    render_segment_by_index(
                        src, seg_clips[i], ow, oh, tb, fps_num,
                        fps_den, seg_paths[i])
                except ExportError as e:
                    print(f"job {jid}: segment render detail: {e}")
                    raise RenderError(
                        "We couldn't render this cut. "
                        "Please try again.")
                seg_frames.append(
                    len(probe_frame_times(seg_paths[i])))
        if any(not n for n in seg_frames):
            raise RenderError(
                "We couldn't render this cut. Please try again.")
        fade_secs = [fade_sec if f else 0.0 for f in fades]
        fade_frames = fade_frames_from(fade_sec, fps_num, fps_den)
        planned = (sum(seg_frames)
                   - fade_frames * sum(1 for f in fades if f))
        video_total = planned * fps_den / fps_num
        audio_total = (sum(b - a for a, b in keeps)
                       - fade_sec * sum(1 for f in fades if f))
        total = max(video_total, audio_total)
        cmd, planned_join = join_export_video(
            seg_paths, seg_frames, fade_secs, fps_num, fps_den, tb,
            ass_path, audio_path, dest, total)
        run_ffmpeg_progress(cmd, total, 50, 92, jid, "rendering")
        return total, planned_join
    # Legacy path: no readable frame rate - plain concat of the
    # trimmed keeps with the host scale + level cap, exactly the
    # graph the old code built for this case (no fades, no settb,
    # no frame plan).
    src_w, src_h = probe_video(src)[:2]
    ow, oh = output_dims(src_w, src_h)
    parts = []
    for i, (a, b) in enumerate(keeps):
        seg = (f"[0:v]trim=start={a:.3f}:end={b:.3f},"
               f"setpts=PTS-STARTPTS")
        seg += (f",scale=w={ow}:h={oh}"
                f":force_original_aspect_ratio=decrease"
                f":force_divisible_by=2")
        parts.append(seg + f"[v{i}]")
    vcur = "v0"
    for i in range(1, len(keeps)):
        parts.append(f"[{vcur}][v{i}]concat=n=2:v=1:a=0[vx{i}]")
        vcur = f"vx{i}"
    if ass_path:
        parts.append(f"[{vcur}]ass={ass_path}[vburn]")
        vcur = "vburn"
    total = sum(b - a for a, b in keeps)
    cmd = ["ffmpeg", "-y", "-i", src, "-i", audio_path,
           "-filter_complex", ";".join(parts),
           "-map", f"[{vcur}]", "-map", "1:a",
           "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "18",
           "-preset", "fast", "-level:v", "4.2",
           "-c:a", "aac", "-b:a", "160k",
           "-movflags", "+faststart", "-t", f"{total:.3f}", dest]
    run_ffmpeg_progress(cmd, total, 30, 92, jid, "rendering")
    return total, None


def run_ffmpeg_progress(cmd, total_seconds, lo, hi, jid, stage):
    """Run ffmpeg with -progress on stdout; map out_time to [lo, hi].
    stderr goes to a temp file so filter messages (silencedetect) and
    error tails stay readable. Returns the stderr text.

    Two anti-freeze guards (2026-10-25: a cut sat at 42% forever):
    -nostdin + stdin DEVNULL so ffmpeg can never block reading commands,
    and a watchdog thread that kills ffmpeg if it emits no progress line
    for STALL_SECONDS, turning a silent wedge into a retryable StallError."""
    import threading
    import time
    err_file = tempfile.NamedTemporaryFile(
        mode="w+", suffix=".log", delete=False, dir=os.environ.get("HOOKCUT_WORKDIR", "/tmp"))
    err_path = err_file.name
    full = cmd[:1] + ["-nostdin", "-progress", "pipe:1", "-nostats"] + cmd[1:]
    proc = subprocess.Popen(full, stdout=subprocess.PIPE, stderr=err_file,
                            stdin=subprocess.DEVNULL, text=True, bufsize=1)
    state = {"last": time.monotonic(), "stop": False, "stalled": False}

    def watchdog():
        while not state["stop"]:
            if time.monotonic() - state["last"] > STALL_SECONDS:
                state["stalled"] = True
                try:
                    proc.kill()
                except Exception:  # noqa: BLE001
                    pass
                return
            time.sleep(5)

    threading.Thread(target=watchdog, daemon=True).start()
    last_pct = -1
    try:
        for line in proc.stdout:
            state["last"] = time.monotonic()
            line = line.strip()
            if line.startswith("out_time_ms=") and total_seconds > 0:
                try:
                    t = int(line.split("=", 1)[1]) / 1_000_000.0
                except ValueError:
                    continue
                pct = lo + (hi - lo) * min(1.0, t / total_seconds)
                if int(pct) != last_pct:
                    last_pct = int(pct)
                    set_progress(jid, pct, stage)
    finally:
        state["stop"] = True
    proc.wait()
    err_file.close()
    with open(err_path) as f:
        err_text = f.read()
    os.unlink(err_path)
    if state["stalled"]:
        # Technical detail to the log; human copy to the job's
        # error field (torture-fix F2).
        print(f"encoder stalled: no progress for "
              f"{STALL_SECONDS}s during {stage} (job {jid})")
        raise StallError(
            "Rendering stalled and was stopped. Please try again.")
    if proc.returncode != 0:
        print(f"ffmpeg failed during {stage} (job {jid}): "
              f"{err_text[-400:]}")
        raise RenderError("We couldn't render this cut. "
                          "Please try again.")
    return err_text


def detect_silences(path, duration, jid, noise="-35dB", min_dur=0.7):
    err_text = run_ffmpeg_progress(
        ["ffmpeg", "-i", path, "-af", f"silencedetect=noise={noise}:d={min_dur}",
         "-f", "null", "-"],
        duration, 15, 29, jid, "analyzing")
    starts = [float(x) for x in re.findall(r"silence_start: ([\d.]+)", err_text)]
    ends = [float(x) for x in re.findall(r"silence_end: ([\d.]+)", err_text)]
    silences = []
    for i, s in enumerate(starts):
        e = ends[i] if i < len(ends) else None
        silences.append((s, e))
    return silences


def build_segments(duration, silences, pad=0.12, min_gap=0.35, min_seg=0.25):
    speech = []
    cur = 0.0
    for s, e in silences:
        if e is None:
            e = duration
        if s > cur:
            speech.append([cur, s])
        cur = max(cur, e)
    if cur < duration:
        speech.append([cur, duration])
    if not speech:
        return [[0.0, duration]], 0
    padded = []
    for a, b in speech:
        padded.append([max(0.0, a - pad), min(duration, b + pad)])
    merged = [padded[0]]
    for a, b in padded[1:]:
        if a - merged[-1][1] < min_gap:
            merged[-1][1] = max(merged[-1][1], b)
        else:
            merged.append([a, b])
    merged = [s for s in merged if s[1] - s[0] >= min_seg] or [[0.0, duration]]
    return merged, len(silences)


def render(src, segments, dest, jid):
    parts = []
    n = len(segments)
    for i, (a, b) in enumerate(segments):
        # Cap output at 1080x1920: cuts are made for social posting, and
        # encoding a 4K source at full size is what OOM-killed the worker
        # on 2026-10-25. force_original_aspect_ratio=decrease never
        # upscales a smaller source.
        parts.append(f"[0:v]trim=start={a:.3f}:end={b:.3f},setpts=PTS-STARTPTS,scale=w=1080:h=1920:force_original_aspect_ratio=decrease:force_divisible_by=2[v{i}]")
        parts.append(f"[0:a]atrim=start={a:.3f}:end={b:.3f},asetpts=PTS-STARTPTS[a{i}]")
    concat_in = "".join(f"[v{i}][a{i}]" for i in range(n))
    parts.append(f"{concat_in}concat=n={n}:v=1:a=1[cv][ca]")
    parts.append("[ca]loudnorm=I=-16:TP=-1.5:LRA=11[outa]")
    graph = ";".join(parts)
    total = sum(b - a for a, b in segments)
    cmd = ["ffmpeg", "-y", "-i", src, "-filter_complex", graph,
           "-map", "[cv]", "-map", "[outa]",
           "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "18",
           "-preset", "fast", "-level:v", "4.2", "-c:a", "aac", "-b:a", "160k",
           "-movflags", "+faststart", "-t", f"{total:.3f}", dest]
    run_ffmpeg_progress(cmd, total, 30, 92, jid, "rendering")


def multiclip_canvas(w, h):
    """The multi-clip render canvas: the first clip's dimensions
    under the render host's 1080x1920 cap, never upscaling (the rule
    worker_render.output_dims applies), forced even for yuv420p.
    Every source is normalized onto this one canvas."""
    if w > 1080 or h > 1920:
        f = min(1080 / w, 1920 / h)
        w, h = int(w * f), int(h * f)
    return max(2, w // 2 * 2), max(2, h // 2 * 2)


def render_multiclip(job, srcs, durations, edl, transcript, tmp, out):
    """Multi-Clip render: the C5 render generalized across files.
    Each EDL keep is rendered from ITS OWN source file (its local
    range = stitched range minus the clip's offset) as a segment
    normalized onto the job's one canvas and the first clip's frame
    rate - the exact per-segment normalization the E7 export path
    applies (render_export_segment), so a cut's first render and
    its later editor exports frame the picture the same way.
    Segments then join with the shared frame-quantized 30 ms
    xfade/acrossfade discipline (join_export_video /
    join_export_audio, settb on every branch), captions come from
    build_captions over the stitched keeps, loudness is the shared
    iterative loudnorm to -14 LUFS, and the fail-closed frame check
    compares the output's frame count against the plan. Returns
    (cues, loud_i, loud_passes, out_dur)."""
    jid = job["id"]
    keep_segs = [s for s in edl if s["action"] == "keep"]
    w0, h0, _tb0, fn0, fd0 = probe_video(srcs[0])
    ow, oh = multiclip_canvas(w0, h0)
    fps_num, fps_den = (fn0, fd0) if fn0 > 0 else (30, 1)
    tb = f"{fps_den}/{fps_num}"
    fps_frac = f"{fps_num}/{fps_den}"
    offsets = clip_offsets(durations)
    seg_paths = []
    for i, s in enumerate(keep_segs):
        k = s.get("clip") or 0
        local = {"source_start": s["start"] - offsets[k],
                 "source_end": s["end"] - offsets[k],
                 "speed": 1.0, "mute": False, "volume": 1.0,
                 "fade_in_ms": 0, "fade_out_ms": 0}
        dest = os.path.join(tmp, f"mcseg{i}.mp4")
        try:
            render_export_segment(srcs[k], local, ow, oh, tb,
                                  fps_frac, dest)
        except ExportError as e:
            print(f"job {jid}: segment render detail: {e}")
            raise RenderError(
                "We couldn't render part of this cut. "
                "Please try again.")
        # Broken-timestamps rescue (torture-fix F2): re-renders dest
        # in place by decode-order index when the pts-trimmed
        # segment is missing most of its frames; the measured
        # counts below read the final file either way.
        segment_frames_or_fallback(
            srcs[k], local, dest, ow, oh, tb, fps_num, fps_den, jid)
        seg_paths.append(dest)
        set_progress(jid, 30 + int(20 * (i + 1) / len(seg_paths)),
                     "rendering")
    seg_frames = [len(probe_frame_times(p)) for p in seg_paths]
    seg_durs = [probe_duration(p) for p in seg_paths]
    fps = fps_num / fps_den
    fade_frames = max(1, int(CROSSFADE_SEC * fps + 0.5))
    fade_sec = fade_frames * fps_den / fps_num
    fades, fade_secs = [], []
    for i in range(len(seg_paths) - 1):
        ok = (seg_durs[i] >= FADE_MIN_SEG_SEC
              and seg_durs[i + 1] >= FADE_MIN_SEG_SEC
              and seg_frames[i] > fade_frames
              and seg_frames[i + 1] > fade_frames)
        fades.append(bool(ok))
        fade_secs.append(fade_sec if ok else 0.0)
    keeps = [(s["start"], s["end"]) for s in keep_segs]
    cues = build_captions(transcript, keeps, fades, fade_sec)
    ass_path = None
    if cues and CAPTIONS_DEFAULT == "burned":
        ass_path = os.path.join(tmp, "captions.ass")
        with open(ass_path, "w") as f:
            f.write(captions_ass(cues, ow, oh))
    joined = join_export_audio(seg_paths, fade_secs, tmp)
    audio_path, loud_i, loud_passes = normalize_loudness(
        joined, tmp, "mc")
    total = sum(seg_durs) - sum(fade_secs)
    cmd, planned = join_export_video(
        seg_paths, seg_frames, fade_secs, fps_num, fps_den, tb,
        ass_path, audio_path, out, total)
    run_ffmpeg_progress(cmd, total, 50, 92, jid, "rendering")
    got = count_output_frames(out)
    if got is not None and abs(got - planned) > 1:
        raise RenderError(
            "The rendered video didn't match the edit plan "
            "and may be incomplete. Please try again.")
    return cues, loud_i, loud_passes, probe_duration(out)


# ---------------------------------------------------------------------------
# C6: script matching (the differentiator) - suggestions only.
#
# For a job linked to a kit (kit_id set, resolved against cut_kits),
# match each kit line to the transcript utterance that says it, and
# store three SUGGESTION outputs on the job (script_match):
#   a. best_hook: when the hook line's best-matching utterance is not
#      the take currently opening the EDL, a suggestion naming the
#      utterance and the EDL keep range(s) covering it. The user
#      toggles it in the UI; the toggle (API) moves those ranges to
#      the front of the stored EDL and re-queues the job, and this
#      worker renders keeps in EDL array order - EDL order is data,
#      never a render-time hack (every C5 chain consumes list order).
#   b. not_filmed: kit lines no utterance matched, verbatim.
#   c. claim_flags: KEPT utterances containing a price, a discount
#      phrase, or a superlative that does NOT appear in the kit text.
#      The review UI links each flag to Violation Shield (/app/shield).
# Matching is deterministic token overlap - no LLM, no embeddings
# (the C6 prompt forbids an embedding service for v1): normalize
# (lowercase, strip punctuation/fillers) and score each utterance by
# the share of the kit line's token set it contains. A line matches
# its best-scoring utterance when the score reaches
# KIT_MATCH_THRESHOLD.
# Jobs without a kit are completely unaffected: no kit_id, no work,
# script_match stays null. A kit_id that resolves to no cut_kits row,
# or any matching failure, degrades to a stored status - never a
# failed job (Hard Rule 4). Computed once: a job that already has
# script_match is never re-matched (the best-hook toggle state the
# user sets lives in the same record, managed by the API).
# Privacy (Hard Rule 5): kit lines and utterance texts are job data
# stored on the job, like the transcript; analytics carry counts only
# and the worker log prints tallies, never line or utterance text.
# ---------------------------------------------------------------------------

KIT_MATCH_THRESHOLD = 0.6  # share of a kit line's tokens an utterance
# must contain to count as that line's match (stated per the prompt).

# The prompt's claim vocabulary, verbatim: superlatives (best,
# cheapest, #1, guaranteed) and discount words (sale, discount,
# coupon, and "off" in its percent-off sense - see _claim_hits).
CLAIM_SUPERLATIVES = ("best", "cheapest", "#1", "guaranteed")
CLAIM_DISCOUNT_WORDS = ("sale", "discount", "coupon")
_PRICE_RE = re.compile(
    r"\$\s*\d[\d,]*(?:\.\d{1,2})?|\b\d+(?:\.\d+)?\s*dollars?\b",
    re.IGNORECASE)
_PCT_OFF_RE = re.compile(
    r"[\w']+\s+percent\s+off|\d+(?:\.\d+)?\s*%\s*off",
    re.IGNORECASE)


def _match_tokens(text):
    """Normalized token set for overlap scoring: lowercase, letters /
    digits only, filler words dropped (the C4 filler vocabulary)."""
    toks = set()
    for raw in re.split(r"[^a-z0-9#]+", (text or "").lower()):
        if raw and raw not in FILLER_WORDS:
            toks.add(raw)
    return toks


def _claim_hits(text, kit_tokens, kit_raw):
    """Claim spans in one utterance that do NOT appear in the kit
    text: prices ($ or number + dollars), percent-off phrases, and
    the prompt's discount / superlative words. A hit is suppressed
    when the kit itself makes the same claim (same word in the kit,
    the same price amount, or a percent-off promise in the kit).
    The bare word "off" is only a claim in its percent-off sense
    (the prompt's example is "forty percent off"); ordinary uses
    ("kicks off", "off the box") are not claims."""
    hits = []
    for m in _PRICE_RE.finditer(text):
        nums = re.findall(r"\d[\d,]*(?:\.\d+)?", m.group(0))
        kit_nums = re.findall(r"\d[\d,]*(?:\.\d+)?", kit_raw)
        if not any(n in kit_nums for n in nums):
            hits.append(m.group(0).strip())
    if not re.search(r"percent\s+off|%\s*off", kit_raw):
        for m in _PCT_OFF_RE.finditer(text):
            hits.append(m.group(0).strip())
    words = set(re.split(r"[^a-z0-9#]+", (text or "").lower()))
    for w in CLAIM_DISCOUNT_WORDS + CLAIM_SUPERLATIVES:
        if w in words and w not in kit_tokens and w not in hits:
            hits.append(w)
    return hits


def compute_script_match(kit, transcript, edl):
    """The three C6 outputs for one job. Pure: transcript utterances
    + the persisted EDL + the kit's verbatim lines in, record out.
    Every time stored is copied from a real Deepgram utterance
    (Hard Rule 1) - matching invents no timestamps."""
    from datetime import datetime, timezone
    utterances = (transcript or {}).get("utterances") or []
    script_lines = kit.get("script_lines") or []
    kit_lines = [("hook", kit.get("hook_text") or "")] + [
        ("script", t) for t in script_lines]
    kit_raw = " ".join([kit.get("hook_text") or ""] + list(script_lines))
    kit_tokens = _match_tokens(kit_raw)
    utt_tokens = [_match_tokens(u.get("transcript")) for u in utterances]

    lines_out, not_filmed = [], []
    for i, (kind, text) in enumerate(kit_lines):
        ltoks = _match_tokens(text)
        best_i, best_score = -1, 0.0
        if ltoks:
            for ui, utoks in enumerate(utt_tokens):
                score = len(ltoks & utoks) / len(ltoks)
                if score > best_score:
                    best_i, best_score = ui, score
        matched = best_i >= 0 and best_score >= KIT_MATCH_THRESHOLD
        entry = {"index": i, "kind": kind, "text": text,
                 "matched": matched,
                 "score": round(best_score, 3) if best_i >= 0 else 0.0,
                 "utterance": None}
        if matched:
            u = utterances[best_i]
            entry["utterance"] = {"start": u["start"], "end": u["end"],
                                  "text": u.get("transcript")}
        else:
            not_filmed.append(text)
        lines_out.append(entry)

    keeps = [s for s in edl if s["action"] == "keep"]
    best_hook = {"suggested": False, "applied": False,
                 "utterance": None, "keep_ranges": []}
    hook = lines_out[0] if lines_out else None
    if hook and hook["matched"] and hook["utterance"]:
        hu = hook["utterance"]
        targets = [s for s in keeps
                   if min(s["end"], hu["end"])
                   - max(s["start"], hu["start"]) > 0.05]
        if targets:
            opening = keeps[0] if keeps else None
            is_opening = (opening is not None
                          and (opening["start"], opening["end"])
                          == (targets[0]["start"], targets[0]["end"]))
            best_hook = {
                "suggested": not is_opening, "applied": False,
                "utterance": hu,
                "keep_ranges": [[s["start"], s["end"]] for s in targets]}

    claim_flags = []
    for u in utterances:
        dur = (u.get("end") or 0) - (u.get("start") or 0)
        if dur <= 0:
            continue
        overlap = sum(
            max(0.0, min(u["end"], s["end"]) - max(u["start"], s["start"]))
            for s in keeps)
        if overlap < 0.5 * dur:
            continue  # not a kept utterance - flags cover what ships
        claims = _claim_hits(u.get("transcript") or "",
                             kit_tokens, kit_raw.lower())
        if claims:
            claim_flags.append({"text": u.get("transcript"),
                                "start": u["start"], "end": u["end"],
                                "claims": claims})

    return {"version": 1, "status": "ok",
            "threshold": KIT_MATCH_THRESHOLD,
            "lines": lines_out, "best_hook": best_hook,
            "not_filmed": not_filmed, "claim_flags": claim_flags,
            "matched_at": datetime.now(timezone.utc).isoformat()}


def match_script_job(job, transcript, edl):
    """C6 stage: compute + store script_match for kit-linked jobs.
    Runs once per job (an existing script_match is final, like the
    EDL). Never raises: matching degrades, the job still renders."""
    jid = job["id"]
    if job.get("script_match") is not None or not job.get("kit_id"):
        return
    kit_id = job["kit_id"]
    try:
        rows = rest("GET",
                    f"cut_kits?id=eq.{kit_id}&select=id,hook_text,script_lines")
        kit = rows[0] if rows else None
        if not kit:
            result = {"version": 1, "status": "kit_unavailable",
                      "kit_id": kit_id,
                      "threshold": KIT_MATCH_THRESHOLD}
        else:
            result = compute_script_match(kit, transcript, edl)
            result["kit_id"] = kit_id
        rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
             {"script_match": result})
        if result.get("status") == "ok":
            print(f"job {jid}: script match - "
                  f"{sum(1 for l in result['lines'] if l['matched'])}"
                  f"/{len(result['lines'])} lines matched, "
                  f"{len(result['not_filmed'])} not filmed, "
                  f"{len(result['claim_flags'])} claim flag(s), "
                  f"best-hook suggestion: "
                  f"{result['best_hook']['suggested']}")
        else:
            print(f"job {jid}: script match skipped - kit unavailable")
    except Exception as e:  # noqa: BLE001 - degrade, never fail the job
        print(f"job {jid}: script match unavailable "
              f"({type(e).__name__})")
        try:
            rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
                 {"script_match": {"version": 1,
                                    "status": "unavailable",
                                    "kit_id": kit_id,
                                    "threshold": KIT_MATCH_THRESHOLD}})
        except Exception:  # noqa: BLE001
            pass



# ---------------------------------------------------------------------------
# The Director (Oct 27, 2026): after the cut is made, The Director
# watches the FINISHED cut the way a director would, names the ONE
# missing piece - an unfilmed kit closer, an unanswered hook promise,
# or no call to action - and tells the creator the one short line to
# film. The filmed pickup is transcribed and spliced into the SAME
# job: a pickup never consumes a cut charge (the charge lives only in
# the guarded start transition, which this section never touches).
#
# Detection is two layers, matching the engine's house pattern: a
# deterministic lexicon pass settles the clear cases for free, then
# ONE Claude pass (the C4 discipline - kept utterances numbered by
# their transcript index, never timestamps) judges the rest and
# drafts the line in the creator's own words. Strictness is Matt's
# call: when in doubt, stay silent - "your cut is complete" is a
# first-class verdict, and a drafted line may never invent a price
# or figure (the guard below rejects one that does).
#
# Failure posture mirrors C6 exactly: the analysis stores its outcome
# on the job and NEVER raises into the pipeline - a failed analysis
# stores state=analysis_failed, shows no card, and the job renders
# on. Prompts, transcripts, and drafted lines are job data: logs
# carry states and counts only (Hard Rule 5).
# ---------------------------------------------------------------------------

DIRECTOR_VERSION = 1


class DirectorError(Exception):
    """A Director analysis failure. Internal only: it is always
    caught by the stage/pickup/recheck wrappers and stored as a
    status - it never reaches a job's error field."""


def _director_kept_view(transcript, spans):
    """The finished cut as The Director sees it. `spans` are the kept
    ranges on the stitched axis in output order - (start, end,
    speed): EDL keeps at speed 1 on the initial pass, timeline clips
    on a recheck. An utterance is KEPT when the spans cover at least
    half of it (C6's claim-flag rule, union coverage - a duplicated
    clip counts once). Returns (kept, total_out): kept entries are
    {index, start, end, text, out: [(out_start, out_end)]} in
    transcript order; total_out is the finished cut's duration."""
    utterances = (transcript or {}).get("utterances") or []
    span_out, acc = [], 0.0
    for (s, e, speed) in spans:
        speed = speed or 1.0
        d = max(0.0, (e - s) / speed)
        span_out.append((s, e, speed, acc))
        acc += d
    merged = []
    for (s, e) in sorted((s, e) for (s, e, _sp) in spans):
        if merged and s <= merged[-1][1]:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
        else:
            merged.append((s, e))
    kept = []
    for i, u in enumerate(utterances):
        us, ue = u.get("start"), u.get("end")
        if us is None or ue is None or ue - us <= 0:
            continue
        covered = sum(max(0.0, min(ue, e) - max(us, s))
                      for (s, e) in merged)
        if covered < 0.5 * (ue - us):
            continue
        outs = []
        for (s, e, speed, o0) in span_out:
            cs, ce = max(us, s), min(ue, e)
            if ce - cs > 0.05:
                outs.append((o0 + (cs - s) / speed,
                             o0 + (ce - s) / speed))
        kept.append({"index": i, "start": us, "end": ue,
                     "text": u.get("transcript") or "", "out": outs})
    return kept, acc


def _director_windows(kept, total_out):
    """(hook_window, closing_window) as kept-entry lists. The hook
    window owns the first two output occurrences; the closing window
    overlaps the final 20% of the finished cut, plus the owners of
    the last two occurrences, so even a short cut has a closing
    window to judge."""
    occ = []
    for k_i, k in enumerate(kept):
        for (o0, o1) in k["out"]:
            occ.append((o0, o1, k_i))
    occ.sort()
    hook_idx = {k_i for (_a, _b, k_i) in occ[:2]}
    close_idx = {k_i for (_o0, o1, k_i) in occ
                 if total_out > 0 and o1 > 0.8 * total_out}
    close_idx |= {k_i for (_a, _b, k_i) in occ[-2:]}
    return ([kept[i] for i in sorted(hook_idx)],
            [kept[i] for i in sorted(close_idx)])


# CTA token patterns for the deterministic pass: a call to action is
# an action word meeting its object ("link in bio", "tap the link",
# "use code X") - the spec's vocabulary, as token sets over
# _match_tokens. "subscribe" stands alone: in a closing window it is
# never anything else.
DIRECTOR_CTA_PATTERNS = (
    {"link", "bio"}, {"link", "description"}, {"link", "pinned"},
    {"link", "tap"}, {"link", "click"}, {"link", "check"},
    {"cart", "tap"}, {"cart", "click"}, {"cart", "grab"},
    {"code", "use"}, {"code", "discount"}, {"code", "promo"},
    {"code", "my"}, {"subscribe"},
)

DIRECTOR_PROMISE_PHRASES = (
    "let me show you", "watch till", "watch until", "until the end",
    "wait for it", "here's why", "here's how", "the result",
    "find out", "stay tuned", "you won't believe")

DIRECTOR_QUESTION_STARTERS = {
    "who", "what", "when", "where", "why", "how", "can", "could",
    "does", "do", "did", "is", "are", "was", "were", "will", "would",
    "should"}


def _director_cta(text):
    toks = _match_tokens(text)
    return any(p <= toks for p in DIRECTOR_CTA_PATTERNS)


def _director_promise(text):
    t = (text or "").lower()
    if "?" in t:
        return True
    words = [w for w in re.split(r"[^a-z']+", t) if w]
    if words and words[0].strip("'") in DIRECTOR_QUESTION_STARTERS:
        return True
    return any(p in t for p in DIRECTOR_PROMISE_PHRASES)


def _director_deterministic(kept, total_out, kit, script_match):
    """Layer 1 (free, no model): returns ("kit_closer", detail) |
    ("complete", {}) | ("unclear", {}). Conservative by design -
    deterministic-complete requires a CTA pattern in the closing
    window AND no promise shape in the hook window; anything else
    defers to the Claude pass."""
    if kit and script_match and script_match.get("status") == "ok":
        lines = [kit.get("hook_text") or ""] + list(
            kit.get("script_lines") or [])
        if (lines and lines[-1]
                and lines[-1] in (script_match.get("not_filmed")
                                  or [])):
            return "kit_closer", {"line": lines[-1],
                                  "kit_line_index": len(lines) - 1}
    hook, closing = _director_windows(kept, total_out)
    cta = any(_director_cta(k["text"]) for k in closing)
    promise = any(_director_promise(k["text"]) for k in hook)
    if cta and not promise:
        return "complete", {}
    return "unclear", {}


def _director_line_clean(line, *texts):
    """The drafted-line guard: a line may only carry a price or a
    percentage the creator (or the kit) actually stated. Every
    number inside a $ amount or a % figure in the line must appear
    among the numbers in the kept transcript / kit text."""
    allowed = set()
    for t in texts:
        allowed |= set(re.findall(r"\d[\d,]*(?:\.\d+)?", t or ""))
    for m in _PRICE_RE.finditer(line):
        for n in re.findall(r"\d[\d,]*(?:\.\d+)?", m.group(0)):
            if n not in allowed:
                return False
    for m in re.finditer(r"(\d+(?:\.\d+)?)\s*%", line):
        if m.group(1) not in allowed:
            return False
    return True


def build_director_prompt(kept, kit, script_match):
    """The one Claude pass's prompt: kept utterances numbered by
    their transcript index (indices are the only positional language,
    per Hard Rule 1), the kit context when the job is kit-linked,
    and the priority order from the spec."""
    body = "\n".join(f"Utterance {k['index']}: {k['text']}"
                     for k in kept)
    kit_note = ""
    if kit:
        kit_lines = [kit.get("hook_text") or ""] + list(
            kit.get("script_lines") or [])
        rendered = "\n".join(f"- {t}" for t in kit_lines if t)
        not_filmed = (script_match or {}).get("not_filmed") or []
        nf = "\n".join(f"- {t}" for t in not_filmed) or "(none)"
        kit_note = ("\n\nThe creator filmed from a script kit. "
                    "Kit lines:\n" + rendered
                    + "\nKit lines no utterance matched "
                      "(not filmed):\n" + nf)
    return (
        "You are The Director for Hook Lab Cut, reviewing a "
        "FINISHED cut of a creator's talking-head video. Below are "
        "the utterances that survived the edit, numbered by their "
        "transcript index. The numbers are the ONLY way to refer "
        "to positions - never use seconds or timestamps.\n\n"
        + body + kit_note +
        "\n\nJudge the finished cut in this priority order:\n"
        "1. \"kit_closer\": only when a kit is present and its LAST "
        "line is effectively missing from the cut (not filmed, or "
        "filmed so garbled it does not land).\n"
        "2. \"payoff\": the opening utterances promise something - "
        "a question, a tease, a result to show - and no later kept "
        "utterance answers, resolves, or closes it.\n"
        "3. \"cta\": no kept utterance asks the viewer to do "
        "anything (tap the cart or a link, check the link in bio, "
        "comment, follow, use a code).\n"
        "If none of these is missing, the verdict is \"complete\". "
        "When in doubt, say complete - a wrong suggestion is worse "
        "than silence.\n\n"
        "When something is missing, draft THE one line the creator "
        "should film: at most 14 words, in the creator's own words "
        "and style from this transcript, plain second person. "
        "Never invent a price, discount, statistic, or product "
        "claim the transcript or kit does not state. For a payoff, "
        "the line is a bridge that points back to the promise (the "
        "real payoff is footage) - say so in \"why\".\n\n"
        "Respond with ONLY a JSON object, no other text:\n"
        "{\"verdict\": \"complete\"|\"missing\", "
        "\"kind\": \"kit_closer\"|\"payoff\"|\"cta\"|null, "
        "\"promise_utterance\": <utterance index or null>, "
        "\"line\": \"<the one line, when missing>\", "
        "\"why\": \"<one sentence, when missing>\"}")


def _director_claude(kept, kit, script_match):
    """Run the one Claude pass and validate its answer hard (the
    C4 judge's discipline): verdict inside the vocabulary, a
    missing verdict names a real kind, the drafted line is present
    and at most 14 words, a payoff names one of the kept
    utterances. Any contract break raises DirectorError and the
    analysis stores analysis_failed - never a partial verdict."""
    text = _call_claude(build_director_prompt(kept, kit,
                                              script_match))
    t = (text or "").strip()
    s, e = t.find("{"), t.rfind("}")
    if s < 0 or e <= s:
        raise DirectorError("unparseable director response")
    try:
        obj = json.loads(t[s:e + 1])
    except Exception:  # noqa: BLE001 - any parse failure is one failure
        raise DirectorError("unparseable director response")
    if not isinstance(obj, dict):
        raise DirectorError("director response is not an object")
    verdict = obj.get("verdict")
    if verdict == "complete":
        return {"verdict": "complete"}
    if verdict != "missing":
        raise DirectorError("director verdict outside the vocabulary")
    kind = obj.get("kind")
    if kind not in ("kit_closer", "payoff", "cta"):
        raise DirectorError("director kind outside the vocabulary")
    line = obj.get("line")
    why = obj.get("why")
    if not isinstance(line, str) or not line.strip():
        raise DirectorError("director line missing")
    line = line.strip().strip('"').strip()
    if not line or len(line.split()) > 14:
        raise DirectorError("director line over 14 words")
    if not isinstance(why, str) or not why.strip():
        raise DirectorError("director why missing")
    promise = obj.get("promise_utterance")
    if kind == "payoff":
        kept_idx = {k["index"] for k in kept}
        if (not isinstance(promise, int) or isinstance(promise, bool)
                or promise not in kept_idx):
            raise DirectorError(
                "payoff names no kept promise utterance")
    return {"verdict": "missing", "kind": kind, "line": line,
            "why": why.strip()[:300],
            "promise_utterance": promise if kind == "payoff" else None}


def run_director_analysis(job, transcript, spans, kit, script_match,
                          history):
    """The analysis core, shared by the initial pass and the
    recheck pass. Returns the director record to store. Raises
    (DirectorError, or EditError from the Claude call) on any
    failure - callers store analysis_failed and move on."""
    from datetime import datetime, timezone
    kept, total_out = _director_kept_view(transcript, spans)
    if not kept:
        raise DirectorError("no kept utterances to judge")
    rec = {"version": DIRECTOR_VERSION,
           "analyzed_at": datetime.now(timezone.utc).isoformat(),
           "model": None, "history": list(history or [])[-20:]}
    det, detail = _director_deterministic(kept, total_out, kit,
                                           script_match)
    if det == "kit_closer":
        rec.update({
            "state": "missing", "kind": "kit_closer",
            "line": detail["line"], "line_source": "kit",
            "kit_line_index": detail["kit_line_index"],
            "why": "Your kit's closing line never made it into "
                   "the cut.",
            "evidence": {}, "placement": {"mode": "kit_order"},
            "analysis": {"deterministic": "kit_closer_missing",
                         "claude_used": False}})
        return rec
    if det == "complete":
        rec.update({
            "state": "complete",
            "analysis": {"deterministic": "cta_found",
                         "claude_used": False}})
        return rec
    verdict = _director_claude(kept, kit, script_match)
    rec["model"] = EDIT_MODEL
    rec["analysis"] = {"deterministic": "unclear", "claude_used": True}
    if verdict["verdict"] == "complete":
        rec["state"] = "complete"
        return rec
    kind = verdict["kind"]
    if kind == "kit_closer":
        # The line is the kit's own closing line, verbatim (Matt's
        # call): the model's draft is never used for a kit closer.
        lines = []
        if kit:
            lines = [kit.get("hook_text") or ""] + list(
                kit.get("script_lines") or [])
        if not lines or not lines[-1]:
            raise DirectorError("kit_closer verdict without a kit")
        rec.update({
            "state": "missing", "kind": "kit_closer",
            "line": lines[-1], "line_source": "kit",
            "kit_line_index": len(lines) - 1,
            "why": verdict["why"], "evidence": {},
            "placement": {"mode": "kit_order"}})
        return rec
    kept_text = " ".join(k["text"] for k in kept)
    kit_text = ""
    if kit:
        kit_text = " ".join([kit.get("hook_text") or ""]
                            + list(kit.get("script_lines") or []))
    if not _director_line_clean(verdict["line"], kept_text, kit_text):
        raise DirectorError(
            "drafted line carries an unstated price or figure")
    rec.update({"state": "missing", "kind": kind,
                "line": verdict["line"], "line_source": "transcript",
                "why": verdict["why"]})
    if kind == "payoff":
        pu = next(k for k in kept
                  if k["index"] == verdict["promise_utterance"])
        rec["evidence"] = {"promise_utterance": pu["index"]}
        rec["placement"] = {"mode": "after_utterance",
                            "utterance_start": pu["start"],
                            "utterance_end": pu["end"]}
    else:  # cta
        rec["evidence"] = {}
        rec["placement"] = {"mode": "end"}
    return rec


def _director_failed_record(history=None):
    from datetime import datetime, timezone
    return {"version": DIRECTOR_VERSION, "state": "analysis_failed",
            "analyzed_at": datetime.now(timezone.utc).isoformat(),
            "model": None,
            "analysis": {"deterministic": "error",
                         "claude_used": False},
            "history": list(history or [])[-20:]}


def analyze_director_job(job, transcript, edl):
    """The Director's initial-pass stage: compute + store the
    director record for a freshly edited job. Mirrors
    match_script_job's contract exactly - runs once per job (a
    stored director record is final, like the EDL and
    script_match), and never raises: a failed analysis stores
    analysis_failed and the job renders on."""
    jid = job["id"]
    if job.get("director") is not None:
        return
    try:
        rows = rest("GET",
                    f"hookcut_jobs?id=eq.{jid}"
                    "&select=script_match,kit_id")
        fresh = rows[0] if isinstance(rows, list) and rows else {}
        kit = None
        kit_id = fresh.get("kit_id") or job.get("kit_id")
        if kit_id:
            krows = rest("GET",
                         f"cut_kits?id=eq.{kit_id}"
                         "&select=id,hook_text,script_lines")
            kit = krows[0] if isinstance(krows, list) and krows \
                else None
        spans = [(s["start"], s["end"], 1.0) for s in (edl or [])
                 if s.get("action") == "keep"]
        rec = run_director_analysis(job, transcript, spans, kit,
                                    fresh.get("script_match"), [])
        rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
             {"director": rec})
        print(f"job {jid}: director - {rec['state']}"
              + (f" ({rec['kind']})" if rec.get("kind") else ""))
    except Exception as e:  # noqa: BLE001 - degrade, never fail the job
        print(f"job {jid}: director analysis unavailable "
              f"({type(e).__name__})")
        try:
            rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
                 {"director": _director_failed_record()})
        except Exception:  # noqa: BLE001
            pass


def seed_usage(job):
    """Director passes (pickup transcription, recheck) run in a
    FRESH process on a job whose first pass already logged usage.
    Seed the module accumulator from the stored hookcut_job_usage
    row so the note_* upserts CONTINUE the job's totals - an
    unseeded accumulator would clobber the first pass's actuals
    with this pass's small numbers (the upsert merges by job_id).
    Wall times and outcome are never written outside the initial
    pass: these passes persist only through note_deepgram_billed /
    note_claude_usage, which record_job_usage drives with the
    counter fields alone."""
    begin_usage(job)
    try:
        rows = rest("GET",
                    f"hookcut_job_usage?job_id=eq.{job['id']}"
                    "&select=deepgram_billed_sec,"
                    "claude_input_tokens,claude_output_tokens")
        if isinstance(rows, list) and rows:
            r = rows[0]
            USAGE["deepgram_billed_sec"] = float(
                r.get("deepgram_billed_sec") or 0)
            USAGE["claude_input_tokens"] = int(
                r.get("claude_input_tokens") or 0)
            USAGE["claude_output_tokens"] = int(
                r.get("claude_output_tokens") or 0)
    except Exception as e:  # noqa: BLE001 - bookkeeping never fails work
        print(f"usage: could not seed for job {job['id']}: {e}")


# ---------------------------------------------------------------------------
# The Director: pickup transcription pass.
#
# A pickup is a NEW claim path on a READY job, so it claims the
# SOURCE row (kind='pickup', status='uploaded'), never the job: the
# job stays 'ready', its stored output is untouched, its claim
# columns belong to the initial pipeline. The pass transcribes ONLY
# the new clip and appends its words to the job's transcript in
# stitched coordinates (the Multi-Clip offset arithmetic), then
# line-checks what was heard against the line on the card. Claims,
# heartbeats, and stale reclaim follow the Parallel Engines rules;
# attempts are bounded via director.pickup.attempts, after which the
# pickup settles 'failed' and the card can offer a re-upload.
# ---------------------------------------------------------------------------

def _append_transcript(transcript, part, offset, clip_idx, clip_dur):
    """Stitch one more clip onto a stored transcript - the same
    arithmetic stitch_transcripts applies across a take's clips:
    the new clip's words and utterances shift by the offset (a sum
    of measured durations - no fabricated timestamps) and carry the
    new clip index. A one-clip transcript becomes multi-clip: its
    existing entries are stamped with clip 0 and the clips meta
    opens with the original clip, so every consumer sees one
    consistent shape."""
    words = transcript.setdefault("words", [])
    utterances = transcript.setdefault("utterances", [])
    if "clips" not in transcript:
        for w in words:
            w.setdefault("clip", 0)
        for u in utterances:
            u.setdefault("clip", 0)
        transcript["clips"] = [{
            "index": 0, "duration_sec": round(float(offset), 3),
            "words": len(words),
            "request_id": transcript.get("request_id")}]
        transcript["multiclip"] = True
    for w in part["words"]:
        w2 = dict(w)
        w2["start"] = w["start"] + offset
        w2["end"] = w["end"] + offset
        w2["clip"] = clip_idx
        words.append(w2)
    for u in part["utterances"]:
        u2 = dict(u)
        u2["start"] = u["start"] + offset
        u2["end"] = u["end"] + offset
        u2["clip"] = clip_idx
        utterances.append(u2)
    if part.get("text"):
        transcript["text"] = (
            (transcript.get("text") or "") + " " + part["text"]).strip()
    transcript["clips"].append({
        "index": clip_idx, "duration_sec": round(float(clip_dur), 3),
        "words": len(part["words"]),
        "request_id": part.get("request_id"), "pickup": True})
    transcript["duration"] = round(float(offset) + float(clip_dur), 3)
    return transcript


def claim_next_pickup():
    """Claim the oldest uploaded pickup row, atomically - the same
    conditional-PATCH discipline as _claim, on the sources table:
    the PATCH lands only while the row is still 'uploaded' and
    unclaimed, so exactly one engine wins."""
    rows = rest("GET",
                "hookcut_job_sources?kind=eq.pickup&status=eq.uploaded"
                "&claimed_by=is.null&select=id"
                "&order=created_at.asc&limit=5")
    if not isinstance(rows, list):
        return None
    for row in rows:
        got = rest("PATCH",
                   f"hookcut_job_sources?id=eq.{row['id']}"
                   "&status=eq.uploaded&claimed_by=is.null",
                   {"status": "transcribing", "claimed_by": ENGINE_ID,
                    "claimed_at": "now()"})
        if isinstance(got, list) and got:
            return got[0]
    return None


def _settle_pickup_failure(src_row, job, director, note):
    """Terminal pickup failure: the row settles 'failed' and the
    card's pickup status follows. The job itself is never touched
    beyond its director record."""
    sid = src_row["id"]
    rest("PATCH", f"hookcut_job_sources?id=eq.{sid}",
         {"status": "failed", "claimed_by": None, "claimed_at": None})
    if job is not None and director is not None:
        pickup = dict(director.get("pickup") or {})
        pickup.update({"status": "failed", "source_id": sid})
        d2 = dict(director)
        d2["pickup"] = pickup
        try:
            rest("PATCH", f"hookcut_jobs?id=eq.{job['id']}",
                 {"director": d2})
        except Exception:  # noqa: BLE001 - the row state is the truth
            pass
    print(f"pickup {sid}: failed ({note})")


def _release_pickup(src_row, job=None, director=None, row_guard=None):
    """Non-terminal pickup failure: count the attempt on the
    director record and return the row to 'uploaded', claim-free,
    for a later pass - or settle it failed at the attempt cap.
    `row_guard` (a PostgREST filter fragment) carries the
    staleness condition when called by the reclaimer, so a row
    that moved in the meantime is never stolen back."""
    sid = src_row["id"]
    attempts = int(((director or {}).get("pickup") or {})
                   .get("attempts") or 0) + 1
    guard = f"&{row_guard}" if row_guard else ""
    if attempts >= MAX_JOB_ATTEMPTS:
        rest("PATCH",
             f"hookcut_job_sources?id=eq.{sid}{guard}",
             {"status": "failed", "claimed_by": None,
              "claimed_at": None})
        if job is not None and director is not None:
            pickup = dict(director.get("pickup") or {})
            pickup.update({"status": "failed", "source_id": sid,
                           "attempts": attempts})
            d2 = dict(director)
            d2["pickup"] = pickup
            try:
                rest("PATCH", f"hookcut_jobs?id=eq.{job['id']}",
                     {"director": d2})
            except Exception:  # noqa: BLE001
                pass
        print(f"pickup {sid}: failed after {attempts} attempts")
        return
    if job is not None and director is not None:
        pickup = dict(director.get("pickup") or {})
        pickup["attempts"] = attempts
        d2 = dict(director)
        d2["pickup"] = pickup
        try:
            rest("PATCH", f"hookcut_jobs?id=eq.{job['id']}",
                 {"director": d2})
        except Exception:  # noqa: BLE001
            pass
    rest("PATCH", f"hookcut_job_sources?id=eq.{sid}{guard}",
         {"status": "uploaded", "claimed_by": None,
          "claimed_at": None})
    print(f"pickup {sid}: released for retry (attempt {attempts})")


def process_pickup(src_row):
    """Transcribe one claimed pickup and line-check it. The job's
    status, progress, output keys, and allowance are NEVER touched;
    the only job writes are transcript_words (appended),
    duration_sec (the new stitched total), and the director
    record's pickup block."""
    import copy as _copy
    sid, jid = src_row["id"], src_row["job_id"]
    stop = start_heartbeat("hookcut_job_sources", sid)
    tmp = tempfile.mkdtemp(dir="/home/hatch/workspace/hookcut")
    job = None
    director = None
    try:
        rows = rest("GET", f"hookcut_jobs?id=eq.{jid}")
        job = rows[0] if isinstance(rows, list) and rows else None
        if job is None:
            _settle_pickup_failure(src_row, None, None, "job gone")
            return
        director = job.get("director") or {}
        pickup0 = director.get("pickup") or {}
        if (job.get("status") != "ready"
                or pickup0.get("source_id") != sid):
            _settle_pickup_failure(src_row, job, director,
                                   "job state changed")
            return
        seed_usage(job)
        dest = os.path.join(tmp, "pickup.mp4")
        download(src_row["storage_key"], dest)
        dur = probe_duration(dest)
        audio = os.path.join(tmp, "audio.wav")
        extract_audio(dest, audio)
        part = deepgram_transcribe(audio)
        if not part["words"]:
            pickup = dict(pickup0)
            pickup.update({"status": "failed", "source_id": sid,
                           "heard_text": "", "overlap": 0.0})
            d2 = dict(director)
            d2["pickup"] = pickup
            rest("PATCH", f"hookcut_job_sources?id=eq.{sid}",
                 {"status": "failed", "claimed_by": None,
                  "claimed_at": None})
            rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
                 {"director": d2})
            print(f"pickup {sid}: job {jid} - no speech heard")
            return
        # The stitched offset is the sum of the take sources'
        # measured durations (process() stores them on the rows);
        # fall back to the job's stored total when a row predates
        # that bookkeeping.
        trows = rest("GET",
                     f"hookcut_job_sources?job_id=eq.{jid}"
                     "&kind=eq.take&select=duration_sec"
                     "&order=idx.asc")
        offset, complete = 0.0, isinstance(trows, list) \
            and len(trows) > 0
        if complete:
            for tr in trows:
                if tr.get("duration_sec") is None:
                    complete = False
                    break
                offset += float(tr["duration_sec"])
        if not complete:
            offset = float(job.get("duration_sec")
                           or (job.get("transcript_words") or {})
                           .get("duration") or 0.0)
        transcript = _copy.deepcopy(job["transcript_words"])
        _append_transcript(transcript, part, offset,
                           src_row["idx"], dur)
        heard_text = " ".join(
            (u.get("transcript") or "")
            for u in part["utterances"]).strip() \
            or (part.get("text") or "")
        ltoks = _match_tokens(director.get("line") or "")
        htoks = _match_tokens(heard_text)
        overlap = (len(ltoks & htoks) / len(ltoks)) if ltoks else 0.0
        status = "heard" if overlap >= KIT_MATCH_THRESHOLD \
            else "mismatch"
        pickup = dict(pickup0)
        pickup.update({
            "status": status, "source_id": sid,
            "source_idx": src_row["idx"], "heard_text": heard_text,
            "overlap": round(overlap, 3),
            "span_start": round(part["words"][0]["start"] + offset, 3),
            "span_end": round(part["words"][-1]["end"] + offset, 3)})
        pickup.pop("attempts", None)
        d2 = dict(director)
        d2["pickup"] = pickup
        # Row first, then the job record (the plan's finish order).
        rest("PATCH", f"hookcut_job_sources?id=eq.{sid}",
             {"status": "done", "duration_sec": round(dur, 3),
              "claimed_by": None, "claimed_at": None})
        rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
             {"transcript_words": transcript,
              "duration_sec": round(offset + dur, 3),
              "director": d2})
        print(f"pickup {sid}: job {jid} transcribed - {status} "
              f"(overlap {overlap:.2f}, "
              f"{len(part['words'])} words appended)")
    except Exception as e:  # noqa: BLE001 - bounded retry, never a job failure
        print(f"pickup {sid}: ERROR: {e!r}")
        try:
            _release_pickup(src_row, job, director)
        except Exception:  # noqa: BLE001
            pass
    finally:
        stop()
        end_usage()
        subprocess.run(["rm", "-rf", tmp])


def reclaim_stale_pickups():
    """A pickup left 'transcribing' with a claim stamp frozen past
    STALE_JOB_MINUTES belongs to a dead engine: hand it back
    through the same attempt-bounded release the live path uses.
    The rescue writes carry the staleness condition they were read
    under."""
    from datetime import datetime, timedelta, timezone
    from urllib.parse import quote
    cutoff = (datetime.now(timezone.utc)
              - timedelta(minutes=STALE_JOB_MINUTES)).isoformat()
    q = quote(cutoff, safe=":")
    rows = rest("GET",
                "hookcut_job_sources?kind=eq.pickup"
                f"&status=eq.transcribing&claimed_at=lt.{q}"
                "&select=id,job_id,idx,storage_key&limit=10")
    if not isinstance(rows, list):
        return
    for row in rows or []:
        job, director = None, None
        try:
            jrows = rest("GET",
                         f"hookcut_jobs?id=eq.{row['job_id']}"
                         "&select=id,user_id,director,status")
            if isinstance(jrows, list) and jrows:
                job = jrows[0]
                director = job.get("director")
        except Exception:  # noqa: BLE001 - release proceeds jobless
            pass
        _release_pickup(row, job, director,
                        row_guard=f"claimed_at=lt.{q}")


# ---------------------------------------------------------------------------
# The Director: recheck pass.
#
# The member-facing "Check again" sets director.state to
# 'recheck_requested'; this pass claims the job row by stamping
# claimed_by/claimed_at ONLY - status stays 'ready', so the row is
# invisible to the job pipeline (which claims status=queued rows)
# and to the stale job reclaimer (which keys on active statuses).
# The claim is always released, finished or failed. The fresh
# analysis judges the CURRENT timeline when the member has one -
# their edits are the finished cut now - and any pickup already on
# the record is carried over: splicing and transcription are not
# this pass's business.
# ---------------------------------------------------------------------------

def _timeline_spans(job):
    """Kept ranges for a recheck: the current timeline's clips when
    the member has one, else the stored EDL keeps (no timeline yet
    means no member edits, so the EDL is the current cut)."""
    tl = job.get("timeline") or {}
    clips = tl.get("clips") or []
    if clips:
        return [(float(c["source_start"]), float(c["source_end"]),
                 float(c.get("speed") or 1.0)) for c in clips]
    return [(s["start"], s["end"], 1.0) for s in (job.get("edl")
                                                  or [])
            if s.get("action") == "keep"]


def claim_next_recheck():
    """Claim one ready job whose director record asks for a
    recheck, atomically: the PATCH lands only while the row is
    still unclaimed and still asking. The jsonb arrow filter is
    percent-encoded in the URLs here and in
    reclaim_stale_rechecks: the egress proxy drops the raw ->>
    operator (empty reply); the encoded form decodes to the same
    PostgREST filter."""
    rows = rest("GET",
                "hookcut_jobs?status=eq.ready"
                "&director-%3E%3Estate=eq.recheck_requested"
                "&claimed_by=is.null&select=id"
                "&order=updated_at.asc&limit=5")
    if not isinstance(rows, list):
        return None
    for row in rows:
        got = rest("PATCH",
                   f"hookcut_jobs?id=eq.{row['id']}"
                   "&claimed_by=is.null"
                   "&director-%3E%3Estate=eq.recheck_requested",
                   {"claimed_by": ENGINE_ID, "claimed_at": "now()"})
        if isinstance(got, list) and got:
            return got[0]
    return None


def process_recheck(job):
    jid = job["id"]
    stop = start_heartbeat("hookcut_jobs", jid)
    old = job.get("director") or {}
    from datetime import datetime, timezone
    history = list(old.get("history") or [])
    history.append({"event": "rechecked",
                    "at": datetime.now(timezone.utc).isoformat()})
    try:
        seed_usage(job)
        transcript = job.get("transcript_words")
        if not transcript:
            raise DirectorError("recheck without a transcript")
        kit = None
        if job.get("kit_id"):
            krows = rest("GET",
                         f"cut_kits?id=eq.{job['kit_id']}"
                         "&select=id,hook_text,script_lines")
            kit = krows[0] if isinstance(krows, list) and krows \
                else None
        rec = run_director_analysis(job, transcript,
                                    _timeline_spans(job), kit,
                                    job.get("script_match"), history)
        if old.get("pickup"):
            rec["pickup"] = old["pickup"]
        rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
             {"director": rec})
        print(f"job {jid}: director recheck - {rec['state']}"
              + (f" ({rec['kind']})" if rec.get("kind") else ""))
    except Exception as e:  # noqa: BLE001 - degrade, never fail the job
        print(f"job {jid}: director recheck failed "
              f"({type(e).__name__})")
        try:
            rec = _director_failed_record(history)
            if old.get("pickup"):
                rec["pickup"] = old["pickup"]
            rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
                 {"director": rec})
        except Exception:  # noqa: BLE001
            pass
    finally:
        stop()
        end_usage()
        try:
            rest("PATCH",
                 f"hookcut_jobs?id=eq.{jid}"
                 f"&claimed_by=eq.{ENGINE_ID}",
                 {"claimed_by": None, "claimed_at": None})
        except Exception:  # noqa: BLE001 - release is best-effort;
            pass  # the stale reclaimer is the backstop


def reclaim_stale_rechecks():
    """Release recheck claims whose engine died (claim stamp frozen
    past STALE_JOB_MINUTES, claim still held). Each release is
    counted on the job's director history; the third settles the
    record as analysis_failed instead of retrying forever. The
    writes carry the staleness condition they were read under, so
    a claim a live engine just refreshed is never stolen."""
    from datetime import datetime, timedelta, timezone
    from urllib.parse import quote
    cutoff = (datetime.now(timezone.utc)
              - timedelta(minutes=STALE_JOB_MINUTES)).isoformat()
    q = quote(cutoff, safe=":")
    rows = rest("GET",
                "hookcut_jobs?status=eq.ready"
                "&director-%3E%3Estate=eq.recheck_requested"
                "&claimed_by=not.is.null"
                f"&claimed_at=lt.{q}&select=id,director&limit=10")
    if not isinstance(rows, list):
        return
    for row in rows or []:
        director = dict(row.get("director") or {})
        history = list(director.get("history") or [])
        stale = sum(1 for h in history
                    if h.get("event") == "recheck_stale")
        now = datetime.now(timezone.utc).isoformat()
        history.append({"event": "recheck_stale", "at": now})
        director["history"] = history[-20:]
        if stale + 1 >= MAX_JOB_ATTEMPTS:
            director["state"] = "analysis_failed"
            print(f"reclaim: director recheck {row['id']} failed "
                  f"after repeated stale claims")
        else:
            print(f"reclaim: director recheck {row['id']} claim "
                  f"released")
        rest("PATCH",
             f"hookcut_jobs?id=eq.{row['id']}&claimed_at=lt.{q}",
             {"director": director, "claimed_by": None,
              "claimed_at": None})


# ---------------------------------------------------------------------------
# C7: allowance refund + analytics events.
#
# The worker owns the terminal transitions, so it owns their side
# effects (briefing Rule 4 + the C7 analytics set):
#   - a job that ENDS in status=failed refunds its edit charge
#     automatically: edits_charged returns to 0 on the job row and the
#     cut_usage ledger row is marked refunded; and
#   - cut_ready (with the processing duration) and cut_failed (with the
#     step name) are logged to cut_events.
# Analytics carry counts and durations only (Rule 5) - never transcript
# text, never filenames. Both helpers are best-effort: an analytics or
# ledger hiccup must never change a job's outcome.


# ---------------------------------------------------------------------------
# Per-job usage logging (Owner Portal cost table, Oct 27 2026).
#
# What each job/export ACTUALLY consumed, recorded at the moment the
# provider reports it: Deepgram billed seconds come from the response
# metadata (the figure Deepgram bills on); Claude tokens come from
# the Messages usage object (before this, _call_claude discarded it
# and C8 had to replay prompts to estimate tokens). Stage wall times
# feed the portal's fully-loaded compute allocation. Rows are
# upserted progressively - right inside the provider calls and again
# at stage boundaries - so a job that fails mid-pipeline still
# carries the usage it consumed (a refunded allowance does not
# refund the COGS). One worker process handles exactly one unit
# (see main()), so a module-level accumulator is the running total
# for that unit. Bookkeeping is best-effort like log_cut_event: it
# never fails a job.
# ---------------------------------------------------------------------------

USAGE = {"deepgram_billed_sec": 0.0, "claude_input_tokens": 0,
         "claude_output_tokens": 0, "claude_calls": 0}
USAGE_JOB = {"job": None}  # the hookcut_jobs row being processed


def begin_usage(job):
    USAGE.update({"deepgram_billed_sec": 0.0, "claude_input_tokens": 0,
                  "claude_output_tokens": 0, "claude_calls": 0})
    USAGE_JOB["job"] = job


def end_usage():
    USAGE_JOB["job"] = None


def _usage_upsert(table, key_field, key_value, fields):
    try:
        payload = dict(fields)
        payload[key_field] = key_value
        payload["updated_at"] = "now()"
        sb_req("POST", f"/rest/v1/{table}?on_conflict={key_field}",
               payload,
               {"Prefer": "resolution=merge-duplicates,return=minimal"})
    except Exception as e:  # noqa: BLE001 - bookkeeping never fails work
        print(f"usage: could not record {table} {key_value}: {e}")


def record_job_usage(fields, job=None):
    job = job or USAGE_JOB["job"]
    if not job:
        return
    payload = {"user_id": job.get("user_id")}
    payload.update(fields)
    _usage_upsert("hookcut_job_usage", "job_id", job["id"], payload)


def record_export_usage(exp_id, job, fields):
    if not job:
        return
    payload = {"job_id": job["id"], "user_id": job.get("user_id")}
    payload.update(fields)
    _usage_upsert("hookcut_export_usage", "export_id", exp_id, payload)


def note_deepgram_billed(duration_sec):
    """A Deepgram call just returned: accumulate the provider-reported
    billed duration and persist it immediately - a worker that dies
    later in the pipeline must not lose what was already paid for."""
    if not duration_sec:
        return
    USAGE["deepgram_billed_sec"] += float(duration_sec)
    record_job_usage(
        {"deepgram_billed_sec": round(USAGE["deepgram_billed_sec"], 3)})


def note_claude_usage(usage):
    """An Anthropic Messages call just returned: accumulate the token
    usage the API reported and persist it immediately."""
    if not usage:
        return
    USAGE["claude_calls"] += 1
    if usage.get("input_tokens") is not None:
        USAGE["claude_input_tokens"] += int(usage["input_tokens"])
    if usage.get("output_tokens") is not None:
        USAGE["claude_output_tokens"] += int(usage["output_tokens"])
    record_job_usage(
        {"claude_input_tokens": USAGE["claude_input_tokens"],
         "claude_output_tokens": USAGE["claude_output_tokens"]})


def log_cut_event(job, event, duration_sec=None, step=None):
    try:
        payload = {"event": event, "user_id": job.get("user_id"),
                   "job_id": job.get("id")}
        if duration_sec is not None:
            payload["duration_sec"] = round(float(duration_sec), 2)
        if step is not None:
            payload["step"] = step
        rest("POST", "cut_events", payload)
    except Exception as e:  # noqa: BLE001
        print(f"events: could not log {event} for job {job.get('id')}: {e}")


def refund_charge(job):
    """Mark the job's cut_usage ledger row refunded. The job row's own
    edits_charged is returned to 0 by the caller's status patch."""
    if not job.get("edits_charged"):
        return
    try:
        rest("PATCH",
             f"cut_usage?job_id=eq.{job['id']}&refunded_at=is.null",
             {"refunded_at": "now()"})
    except Exception as e:  # noqa: BLE001
        print(f"job {job.get('id')}: allowance refund update failed: {e}")


def fail_job(job, err, step):
    """Terminal failure: status=failed with the plain-language error,
    and a charged edit refunded automatically (Rule 4 - an allowance
    is never kept for a failed job). A job that is only re-queued for
    a retry never passes through here, so retries keep their charge."""
    jid = job["id"]
    patch = {"status": "failed", "error": str(err)[:500],
             "stage": "error", "finished_at": "now()"}
    if job.get("edits_charged"):
        patch["edits_charged"] = 0
    rest("PATCH", f"hookcut_jobs?id=eq.{jid}", patch)
    if job.get("edits_charged"):
        refund_charge(job)
    log_cut_event(job, "cut_failed", step=step)


def process(job):
    jid = job["id"]
    t0 = time.time()
    begin_usage(job)
    step = "transcribing"  # C7: the step name a cut_failed event reports
    # C3: a job with no transcript enters at 'transcribing' (spec status
    # flow uploaded -> transcribing -> editing -> rendering -> ready);
    # a re-queued job that already has one goes straight to editing.
    first_status = "editing" if job.get("transcript_words") else "transcribing"
    rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
         {"status": first_status, "started_at": "now()", "progress": 1,
          "stage": "starting"})
    tmp = tempfile.mkdtemp(dir=os.environ.get("HOOKCUT_WORKDIR", "/tmp"))
    out = os.path.join(tmp, "cut.mp4")
    try:
        set_progress(jid, 2, "downloading")
        # Multi-Clip Jobs: download every source in play order and
        # probe each one. The 2 GB ceiling stays per file (the rule
        # sign enforces); the 15-minute cap is the TOTAL across all
        # the job's clips, checked on the measured durations.
        sources = job_sources(job)
        srcs, durations = [], []
        for k, s in enumerate(sources):
            dest = os.path.join(tmp, f"source{k}.mp4")
            download(s["storage_key"], dest)
            if os.path.getsize(dest) > 2 * 1024 * 1024 * 1024:
                raise RuntimeError(
                    "This file is over the 2 GB limit for Cut. "
                    "Please export a smaller file and try again.")
            srcs.append(dest)
            durations.append(probe_duration(dest))
            if s.get("row_id"):
                # The probed duration replaces the browser-reported
                # one on the source row, so the timeline conversion
                # places every range in measured spans.
                rest("PATCH",
                     f"hookcut_job_sources?id=eq.{s['row_id']}",
                     {"duration_sec": round(durations[-1], 3)})
        dur = sum(durations)
        if dur > 900:
            if len(srcs) == 1:
                raise RuntimeError(
                    "This video is over the 15-minute limit for Cut. "
                    "Please trim it to 15 minutes or less and try again.")
            raise RuntimeError(
                "These clips add up to more than the 15-minute limit "
                "for Cut - the limit is 15 minutes across all the "
                "clips in one job. Please remove a clip or trim them "
                "and try again.")
        src = srcs[0]
        # C3: transcription. Runs before everything else that uses the
        # media; on failure the job is failed here (never left sitting in
        # 'transcribing', never silently cut without a transcript).
        # Multi-clip jobs transcribe per file and stitch (C3 block).
        had_transcript = bool(job.get("transcript_words"))
        t_tr = time.time()
        transcript = transcribe_job_sources(job, srcs, durations, tmp)
        if not had_transcript:
            record_job_usage(
                {"transcribe_wall_sec": round(time.time() - t_tr, 2)})
        rest("PATCH", f"hookcut_jobs?id=eq.{jid}", {"status": "editing"})
        step = "editing"
        # Tight recipe (Matt's call 2026-10-25 after reviewing the first
        # IMG_0081 cut: pacing too loose, tail after he stopped talking
        # survived). More sensitive detector, shorter minimum pause, less
        # padding kept around each cut. Multi-clip: measured per file
        # and stitched by the same offsets as the transcript.
        if len(srcs) == 1:
            silences = detect_silences(src, dur, jid, noise="-30dB",
                                        min_dur=0.4)
        else:
            silences = []
            off = 0.0
            for k, p in enumerate(srcs):
                for ds, de in detect_silences(p, durations[k], jid,
                                              noise="-30dB", min_dur=0.4):
                    silences.append(
                        (ds + off, de + off if de is not None else None))
                off += durations[k]
        # C4: edit-decision pass (rules + Claude) -> persisted EDL with
        # llm_pass set. On EditError the job is failed by the handlers
        # below - it never renders from a partial or invented EDL.
        had_edl = bool(job.get("edl"))
        t_ed = time.time()
        edl = edit_job(job, transcript, silences, dur)
        if not had_edl:
            record_job_usage(
                {"edit_wall_sec": round(time.time() - t_ed, 2)})
        # C6: script matching for kit-linked jobs (suggestions
        # stored on the job; never alters the EDL or the render).
        match_script_job(job, transcript, edl)
        # The Director: judge the finished cut (this EDL) and
        # store the verdict on the job; never alters the render.
        analyze_director_job(job, transcript, edl)
        # C5: render from the persisted EDL (above). Only the keep
        # ranges are rendered, in order; the prototype silence
        # segments no longer drive any render.
        keeps = [(s["start"], s["end"]) for s in edl
                 if s["action"] == "keep"]
        ncuts = sum(1 for s in edl if s["action"] == "cut")
        if not keeps:
            raise RenderError(
                "The edit pass kept none of the video, so there's "
                "nothing to render. Please try again.")
        t_rn = time.time()
        if len(srcs) > 1:
            step = "rendering"
            rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
                 {"status": "rendering"})
            cues, loud_i, loud_passes, out_dur = render_multiclip(
                job, srcs, durations, edl, transcript, tmp, out)
            srt_path = None
            if cues:
                srt_path = os.path.join(tmp, "captions.srt")
                with open(srt_path, "w") as f:
                    f.write(captions_srt(cues))
        else:
            w_px, h_px, tb, fps_num, fps_den = probe_video(src)
            frame_times = None
            if fps_num > 0:
                try:
                    frame_times = probe_frame_times(src)
                except Exception:  # noqa: BLE001 - planning falls back
                    frame_times = None  # to duration estimates
            fades, fade_sec, seg_frames = fade_plan(
                keeps, fps_num, fps_den, frame_times)
            cues = build_captions(transcript, keeps, fades, fade_sec)
            ass_path = None
            srt_path = None
            if cues:
                cap_w, cap_h = output_dims(w_px, h_px)
                if CAPTIONS_DEFAULT == "burned":
                    ass_path = os.path.join(tmp, "captions.ass")
                    with open(ass_path, "w") as f:
                        f.write(captions_ass(cues, cap_w, cap_h))
                srt_path = os.path.join(tmp, "captions.srt")
                with open(srt_path, "w") as f:
                    f.write(captions_srt(cues))
            audio_path, loud_i, loud_passes = build_output_audio(
                src, keeps, fades, tmp, fade_sec)
            step = "rendering"
            rest("PATCH", f"hookcut_jobs?id=eq.{jid}", {"status": "rendering"})
            _total, planned_frames = render_from_edl(
                src, keeps, fades, ass_path, audio_path,
                out, jid, tb, fps_num, fps_den, fade_sec, tmp)
            out_dur = probe_duration(out)
            if planned_frames is not None:
                # Fail closed: if the joined video lost frames anywhere
                # (a truncated xfade chain), the frame count disagrees
                # with the plan - fail the job instead of shipping a
                # partial render as ready. The plan is now computed
                # from the rendered segments' measured frame counts
                # (torture-fix F2), so it agrees with what the
                # encoder emitted by construction.
                got = count_output_frames(out)
                if got is not None and abs(got - planned_frames) > 1:
                    raise RenderError(
                        "The rendered video didn't match the edit plan "
                        "and may be incomplete. Please try again.")
        record_job_usage(
            {"render_wall_sec": round(time.time() - t_rn, 2)})
        set_progress(jid, 95, "uploading")
        out_path = f"outputs/{jid}.mp4"
        upload(out_path, out)
        captions_key = None
        if srt_path:
            captions_key = f"captions/{jid}.srt"
            upload(captions_key, srt_path, "text/plain; charset=utf-8")
        rest("PATCH", f"hookcut_jobs?id=eq.{jid}", {
            "status": "ready", "output_storage_key": out_path,
            "progress": 100, "stage": "done",
            "duration_sec": round(dur, 3),
            "output_seconds": round(out_dur, 2),
            "cuts": ncuts, "captions_srt_key": captions_key,
            "finished_at": "now()"})
        record_job_usage(
            {"outcome": "ready",
             "processing_wall_sec": round(time.time() - t0, 2)})
        log_cut_event(job, "cut_ready", duration_sec=time.time() - t0)
        print(f"job {jid}: {dur:.1f}s -> {out_dur:.1f}s, {ncuts} EDL cuts, "
              f"{len(cues)} caption cues, loudness "
              f"{loud_i if loud_i is not None else '?'} LUFS "
              f"({loud_passes} loudnorm pass(es))")
    except EditError as e:
        record_job_usage(
            {"outcome": "failed", "failed_step": "editing",
             "processing_wall_sec": round(time.time() - t0, 2)})
        fail_job(job, e, "editing")
        print(f"job {jid} EDIT ERROR: {e}")
    except TranscriptionError as e:
        record_job_usage(
            {"outcome": "failed", "failed_step": "transcribing",
             "processing_wall_sec": round(time.time() - t0, 2)})
        fail_job(job, e, "transcribing")
        print(f"job {jid} TRANSCRIPTION ERROR: {e}")
    except Exception as e:  # noqa: BLE001
        print(f"job {jid} ERROR: {e!r}")
        # Torture-fix F2: technical exception classes that can still
        # reach this handler (ffprobe/subprocess failures on a
        # damaged file, timeouts, undecodable service responses)
        # are mapped to human copy before they can land in the job's
        # error field; the original is in the log line above.
        if isinstance(e, subprocess.TimeoutExpired):
            e = RenderError(
                "Processing this video took too long. "
                "Please try again.")
        elif isinstance(e, (subprocess.CalledProcessError, OSError,
                            json.JSONDecodeError)):
            e = RenderError(
                "We couldn't read this video file. It may be damaged "
                "or in a format we can't open. Please try again.")
        record_job_usage(
            {"outcome": "failed", "failed_step": step,
             "processing_wall_sec": round(time.time() - t0, 2)})
        fail_job(job, e, step)
    finally:
        subprocess.run(["rm", "-rf", tmp])
        end_usage()


# ---------------------------------------------------------------------------
# E7: timeline exports (Editor Prompt E7 step 4).
#
# An export renders the job's CURRENT SAVED Timeline v2 (snapshotted
# onto the cut_exports row by the export API at queue time — never an
# unsaved draft) through the C5 render path extended for the timeline:
# the C5 path's frame-exact xfade join arithmetic, its iterative
# loudnorm loop (normalize_loudness, shared verbatim), and its ASS
# caption approach, generalized to per-clip speed/volume/mute/fades,
# per-join crossfades, the E6 caption style + word highlight, and the
# timeline's text overlays. There is no second renderer: segments are
# rendered per clip (trim + setpts/atempo speed + volume + afade),
# then joined with the same xfade/acrossfade discipline render_from_edl
# uses, with segment frame counts probed from the rendered segments
# so the fail-closed frame check compares plan vs output exactly.
#
# EXPORTS ARE FREE (briefing Rule 7): nothing in this section reads
# or writes cut_usage / edits_charged / the allowance. An export
# failure marks ONLY the cut_exports row failed — the job row (a
# finished, already-charged cut) is never failed or refunded by an
# export. On success the export replaces the job's current output
# (output_storage_key / captions_srt_key / output_seconds); earlier
# exports keep their files under exports/ as versioned outputs.


class ExportError(RuntimeError):
    """An export failure whose message is safe to show the user."""


EXPORT_RES_CAPS = {"1080p": (1080, 1920), "720p": (720, 1280)}

# The E6 caption/overlay font vocabulary -> (ASS Fontname, bold).
# Inter, Barlow Condensed and Source Sans 3 are installed for
# fontconfig on this host (~/.fonts). Where a face is missing,
# libass substitutes its default — the text, timing and position
# are unaffected.
EXPORT_FONT_MAP = {
    "Inter Bold": ("Inter", 1),
    "Inter": ("Inter", 0),
    "Barlow Condensed": ("Barlow Condensed", 0),
    "Source Sans 3": ("Source Sans 3", 0),
}
# The preview's current-word highlight colour (Hook Lab gold
# #FFD21A) in ASS &H00BBGGRR form.
EXPORT_HIGHLIGHT_ASS = "&H001AD2FF"


def set_export_progress(exp_id, pct, stage):
    try:
        rest("PATCH", f"cut_exports?id=eq.{exp_id}",
             {"progress": int(pct), "stage": stage})
    except Exception:  # noqa: BLE001 - progress never fails an export
        pass


def run_ffmpeg_progress_exp(cmd, total_seconds, lo, hi, exp_id, stage):
    """run_ffmpeg_progress's twin for exports: same -progress mapping
    and the same anti-freeze guards (-nostdin + a stall watchdog),
    reported onto the cut_exports row instead of the job row. A stall
    becomes a plain export failure the user can retry."""
    import threading
    import time
    err_file = tempfile.NamedTemporaryFile(
        mode="w+", suffix=".log", delete=False,
        dir=os.environ.get("HOOKCUT_WORKDIR", "/tmp"))
    err_path = err_file.name
    full = cmd[:1] + ["-nostdin", "-progress", "pipe:1", "-nostats"] + cmd[1:]
    proc = subprocess.Popen(full, stdout=subprocess.PIPE, stderr=err_file,
                            stdin=subprocess.DEVNULL, text=True, bufsize=1)
    state = {"last": time.monotonic(), "stop": False, "stalled": False}

    def watchdog():
        while not state["stop"]:
            if time.monotonic() - state["last"] > STALL_SECONDS:
                state["stalled"] = True
                try:
                    proc.kill()
                except Exception:  # noqa: BLE001
                    pass
                return
            time.sleep(5)

    threading.Thread(target=watchdog, daemon=True).start()
    last_pct = -1
    try:
        for line in proc.stdout:
            state["last"] = time.monotonic()
            line = line.strip()
            if line.startswith("out_time_ms=") and total_seconds > 0:
                try:
                    t = int(line.split("=", 1)[1]) / 1_000_000.0
                except ValueError:
                    continue
                pct = lo + (hi - lo) * min(1.0, t / total_seconds)
                if int(pct) != last_pct:
                    last_pct = int(pct)
                    set_export_progress(exp_id, pct, stage)
    finally:
        state["stop"] = True
    proc.wait()
    err_file.close()
    with open(err_path) as f:
        err_text = f.read()
    os.unlink(err_path)
    if state["stalled"]:
        raise ExportError(
            "The export stalled while rendering. Please export again.")
    if proc.returncode != 0:
        # Detail stays in the worker log; the export's error field
        # gets human copy (torture-fix F2).
        print(f"export ffmpeg failed during {stage} "
              f"(export {exp_id}): {err_text[-400:]}")
        raise ExportError("We couldn't render this export. "
                          "Please try again.")
    return err_text


def export_dims(w, h, resolution):
    """Output dimensions for an export: "source" keeps the C5 rule
    (this host's output_dims); "1080p"/"720p" fit inside the named
    frame, never upscaling. Always even dimensions."""
    if resolution in EXPORT_RES_CAPS:
        cap_w, cap_h = EXPORT_RES_CAPS[resolution]
        if w > cap_w or h > cap_h:
            f = min(cap_w / w, cap_h / h)
            w, h = int(w * f), int(h * f)
        else:
            return w - (w % 2), h - (h % 2)
        return max(2, w // 2 * 2), max(2, h // 2 * 2)
    w, h = output_dims(w, h)
    return w - (w % 2), h - (h % 2)


def atempo_chain(speed):
    """Pitch-preserving tempo as an atempo chain (Prompt E7 step 4).
    On this host a single atempo covers 0.5-100, but the chain form
    is correct on any build: factors are multiplied out so each one
    stays inside [0.5, 2.0]."""
    parts = []
    f = float(speed)
    while f > 2.0:
        parts.append(2.0)
        f /= 2.0
    while f < 0.5:
        parts.append(0.5)
        f /= 0.5
    parts.append(round(f, 6))
    return ",".join(f"atempo={p:g}" for p in parts)


def clip_spans_export(timeline):
    """The briefing's preview-time spans: clip i's timeline span is
    the cumulative formula duration of the clips before it."""
    spans = []
    offset = 0.0
    for clip in timeline.get("clips") or []:
        dur = (clip["source_end"] - clip["source_start"]) / clip["speed"]
        spans.append({"clip": clip, "timeline_start": offset,
                      "timeline_end": offset + dur, "duration": dur})
        offset += dur
    return spans


def placed_words_export(timeline, transcript):
    """Mirror of placedWordsForClips in src/lib/cut-timeline.ts:
    a word belongs to a clip when its midpoint falls inside the
    clip's source range (±1 ms), placed by the preview-time mapping.
    Duplicated clips place a word once per clip, exactly as the TS
    does."""
    words = (transcript or {}).get("words") or []
    placed = []
    offset = 0.0
    for clip in timeline.get("clips") or []:
        speed = clip["speed"]
        for i, w in enumerate(words):
            mid = (w["start"] + w["end"]) / 2.0
            if mid < clip["source_start"] - 0.001 \
                    or mid > clip["source_end"] + 0.001:
                continue
            placed.append({
                "index": i,
                "text": w.get("punctuated_word") or w.get("word") or "",
                "timeline_start": round(
                    offset + (w["start"] - clip["source_start"]) / speed, 3),
                "timeline_end": round(
                    offset + (w["end"] - clip["source_start"]) / speed, 3),
            })
        offset += (clip["source_end"] - clip["source_start"]) / speed
    placed.sort(key=lambda p: p["timeline_start"])
    return placed


def export_time_mapper(spans, fade_secs):
    """Map timeline time -> rendered-output time. A faded join
    consumes its fade duration from the output, so everything after
    join j shifts earlier by the fades before it — the same
    accounting keep_offsets does for the C5 render. Returns
    (map_fn, out_starts)."""
    out_starts = []
    consumed = 0.0
    for i, sp in enumerate(spans):
        out_starts.append(sp["timeline_start"] - consumed)
        if i < len(fade_secs):
            consumed += fade_secs[i]

    def map_time(t):
        for i, sp in enumerate(spans):
            if t < sp["timeline_end"] - 1e-9 or i == len(spans) - 1:
                return out_starts[i] + (t - sp["timeline_start"])
        return out_starts[-1] + (t - spans[-1]["timeline_start"])

    return map_time, out_starts


def _ass_rgb(hex_color):
    """"#RRGGBB" -> "BBGGRR" (ASS colour order), uppercase."""
    h = (hex_color or "#FFFFFF").lstrip("#")
    if len(h) != 6:
        h = "FFFFFF"
    return f"{h[4:6]}{h[2:4]}{h[0:2]}".upper()


def _export_fontsize(size, out_h):
    """The E6 style sizes are pixels on the editor's reference canvas
    (the preview scales size by boxWidth/720 on a 9:16 box — i.e.
    size is literal at 720x1280). The export preserves that
    proportion at any output height: fontsize = size * outH / 1280.
    This is the one documented font-metric mapping between the
    preview's approximation and the render (Prompt E7 step 4)."""
    return max(10, int(round(size * out_h / 1280.0)))


def _word_tokens(text):
    return re.findall(r"[a-z0-9']+", (text or "").lower())


def _karaoke_text(mapped_words):
    """ASS karaoke for one cue: each word fills with the highlight
    colour across its own span (\\kf, centiseconds) — the render's
    equivalent of the preview's current-word highlight. mapped_words
    is [(text, out_start, out_end)] on the OUTPUT timeline."""
    parts = []
    for text, s, e in mapped_words:
        cs = max(1, int(round((e - s) * 100)))
        t = text.replace("\\", "").replace("{", "(").replace("}", ")")
        parts.append(f"{{\\kf{cs}}}{t}")
    return " ".join(parts)


def export_ass(timeline, placed, out_w, out_h, map_time, include_captions):
    """The export ASS document: the timeline's stored caption cues in
    the E6 style (font/size/colour/outline/position from
    caption_track.style; word highlight as karaoke when the style
    enables it and the cue's text still aligns word-for-word with
    its stored derivation — a hand-edited cue renders plain, the
    same rule the preview uses) plus the text overlays at their
    timeline times and x/y positions. All times are mapped to the
    OUTPUT timeline (fades consumed) so captions stay in sync with
    the rendered file, exactly as the C5 render's SRT/ASS did.
    Returns (ass_text, dialogue_count)."""
    track = timeline.get("caption_track") or {}
    style = track.get("style") or {}
    cues = (track.get("cues") or []) if include_captions else []
    overlays = timeline.get("text_overlays") or []
    font_name, font_bold = EXPORT_FONT_MAP.get(
        style.get("font"), ("Inter", 0))
    fs = _export_fontsize(style.get("size", 64), out_h)
    outline = max(0, int(round((style.get("outline_px", 4)) * out_h / 1280.0)))
    margin_v = int(round(out_h * 0.03))
    margin_lr = max(10, int(round(out_w * 0.055)))
    align = {"bottom_center": 2, "middle_center": 5,
             "top_center": 8}.get(style.get("position"), 2)
    border_style = 3 if style.get("background") == "black_box" else 1
    back = "&H80000000" if border_style == 3 else "&H00000000"
    highlight = bool(style.get("highlight_current_word"))
    primary = EXPORT_HIGHLIGHT_ASS if highlight else f"&H00{_ass_rgb(style.get('color'))}"
    secondary = f"&H00{_ass_rgb(style.get('color'))}"
    lines = [
        "[Script Info]",
        "Title: Hook Lab Cut export captions",
        "ScriptType: v4.00+",
        f"PlayResX: {out_w}",
        f"PlayResY: {out_h}",
        "WrapStyle: 2",
        "ScaledBorderAndShadow: yes",
        "",
        "[V4+ Styles]",
        ("Format: Name, Fontname, Fontsize, PrimaryColour, "
         "SecondaryColour, OutlineColour, BackColour, Bold, Italic, "
         "Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, "
         "BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, "
         "MarginV, Encoding"),
        (f"Style: Caption,{font_name},{fs},{primary},{secondary},"
         f"&H00{_ass_rgb(style.get('outline_color'))},{back},"
         f"{font_bold},0,0,0,100,100,0,0,{border_style},{outline},0,"
         f"{align},{margin_lr},{margin_lr},{margin_v},1"),
        (f"Style: Overlay,Inter,{_export_fontsize(48, out_h)},"
         "&H00FFFFFF,&H00FFFFFF,&H00000000,&H00000000,0,0,0,0,100,100,"
         "0,0,1,2,0,5,0,0,0,1"),
        "",
        "[Events]",
        ("Format: Layer, Start, End, Style, Name, MarginL, MarginR, "
         "MarginV, Effect, Text"),
    ]
    count = 0
    by_index = {p["index"]: p for p in placed}
    for cue in cues:
        s_out = map_time(cue["timeline_start"])
        e_out = map_time(cue["timeline_end"])
        if e_out <= s_out:
            e_out = s_out + 0.08
        text = None
        if highlight:
            idxs = cue.get("word_indices") or []
            wps = [by_index[i] for i in idxs if i in by_index]
            if wps and len(wps) == len(idxs):
                cue_tokens = _word_tokens(cue.get("text"))
                word_tokens = _word_tokens(
                    " ".join(w["text"] for w in wps))
                if cue_tokens == word_tokens and len(cue_tokens) == len(wps):
                    text = _karaoke_text([
                        (w["text"], map_time(w["timeline_start"]),
                         map_time(w["timeline_end"])) for w in wps])
        if text is None:
            text = _ass_text(cue.get("text") or "")
        lines.append(
            f"Dialogue: 0,{_ass_ts(s_out)},{_ass_ts(e_out)},"
            f"Caption,,0,0,0,,{text}")
        count += 1
    for o in overlays:
        s_out = map_time(o["timeline_start"])
        e_out = map_time(o["timeline_end"])
        if e_out <= s_out:
            continue
        o_font, o_font_bold = EXPORT_FONT_MAP.get(o.get("font"), ("Inter", 0))
        bold = 1 if (o.get("bold") or o_font_bold) else 0
        x = int(round((o.get("x_pct", 50)) / 100.0 * out_w))
        y = int(round((o.get("y_pct", 85)) / 100.0 * out_h))
        o_fs = _export_fontsize(o.get("size", 48), out_h)
        txt = (o.get("text") or "").replace("\\", "").replace(
            "{", "(").replace("}", ")").replace("\n", " ")
        lines.append(
            f"Dialogue: 1,{_ass_ts(s_out)},{_ass_ts(e_out)},"
            f"Overlay,,0,0,0,,{{\\an5\\pos({x},{y})\\fn{o_font}"
            f"\\fs{o_fs}\\c&H{_ass_rgb(o.get('color'))}&\\b{bold}}}{txt}")
        count += 1
    return "\n".join(lines) + "\n", count


def export_srt_cues(timeline, map_time):
    """The export's SRT cues: the timeline's stored caption cues with
    their times mapped onto the OUTPUT timeline (fades consumed) —
    the same mapped times the burned captions use, so the SRT and
    the MP4 can never disagree."""
    cues = []
    for cue in (timeline.get("caption_track") or {}).get("cues") or []:
        s = map_time(cue["timeline_start"])
        e = map_time(cue["timeline_end"])
        if e <= s:
            e = s + 0.08
        cues.append({"start": round(s, 3), "end": round(e, 3),
                     "text": cue.get("text") or ""})
    return [c for c in cues if c["text"]]


def render_export_segment(src, clip, ow, oh, tb, fps_frac, dest):
    """One timeline clip as a stand-alone segment file: trim the
    source range, speed via setpts (video) + the atempo chain
    (audio, pitch-preserving — E5's render statements), volume
    (mute = volume 0; the clip's video still plays), afade in/out
    over the clip's TIMELINE duration, normalized to the output
    dimensions / frame rate / timebase the join chain requires."""
    a, b = float(clip["source_start"]), float(clip["source_end"])
    speed = float(clip.get("speed") or 1.0)
    dur = (b - a) / speed
    vf = (f"trim=start={a:.3f}:end={b:.3f},"
          f"setpts=(PTS-STARTPTS)/{speed:.6f},"
          f"scale={ow}:{oh},fps={fps_frac},format=yuv420p,settb={tb}")
    vol = 0.0 if clip.get("mute") else float(
        clip.get("volume") if clip.get("volume") is not None else 1.0)
    af = [f"atrim=start={a:.3f}:end={b:.3f}", "asetpts=PTS-STARTPTS",
          atempo_chain(speed), f"volume={vol:.4f}"]
    fi = (clip.get("fade_in_ms") or 0) / 1000.0
    fo = (clip.get("fade_out_ms") or 0) / 1000.0
    if fi > 0:
        af.append(f"afade=t=in:st=0:d={min(fi, dur):.3f}")
    if fo > 0:
        af.append(
            f"afade=t=out:st={max(0.0, dur - fo):.3f}:d={min(fo, dur):.3f}")
    cmd = ["ffmpeg", "-y", "-v", "error", "-i", src,
           "-vf", vf, "-af", ",".join(af),
           "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "14",
           "-preset", "fast", "-c:a", "aac", "-b:a", "192k", dest]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    if p.returncode != 0:
        # stderr to the worker log only (torture-fix F2).
        print(f"export segment render failed: {p.stderr[-500:]}")
        raise ExportError(
            "We couldn't render part of this cut for export. "
            "Please try again.")


def render_segment_by_index(src, clip, ow, oh, tb, fps_num,
                            fps_den, dest):
    """Fallback segment render for sources whose video timestamps
    are broken (torture-fix F2, the irregular Main10 fixture class):
    the pts-based trim in render_export_segment finds few or no
    frames in the keep because chunks of the file carry scrambled
    timestamps - multi-second pts holes, with the frames present in
    decode order. Decode order is content order, so select the
    keep's frames by index on the average-rate grid and re-timestamp
    them onto that grid; the audio (whose timestamps are not the
    broken part - the transcript and the EDL are anchored to it) is
    cut by time as usual. Same clip contract as
    render_export_segment (speed, volume and the afade
    in/out are honoured)."""
    a, b = float(clip["source_start"]), float(clip["source_end"])
    speed = float(clip.get("speed") or 1.0)
    dur = (b - a) / speed
    lo = int(a * fps_num / fps_den + 0.5)
    hi = max(lo + 1, int(b * fps_num / fps_den + 0.5))
    vf = (f"select='between(n\\,{lo}\\,{hi - 1})',"
          f"setpts=(N*{fps_den}/({fps_num}*TB))/{speed:.6f},"
          f"scale={ow}:{oh},fps={fps_num}/{fps_den},"
          f"format=yuv420p,settb={tb}")
    vol = 0.0 if clip.get("mute") else float(
        clip.get("volume") if clip.get("volume") is not None else 1.0)
    af = [f"atrim=start={a:.3f}:end={b:.3f}",
          "asetpts=PTS-STARTPTS", atempo_chain(speed),
          f"volume={vol:.4f}"]
    fi = (clip.get("fade_in_ms") or 0) / 1000.0
    fo = (clip.get("fade_out_ms") or 0) / 1000.0
    if fi > 0:
        af.append(f"afade=t=in:st=0:d={min(fi, dur):.3f}")
    if fo > 0:
        af.append(
            f"afade=t=out:st={max(0.0, dur - fo):.3f}"
            f":d={min(fo, dur):.3f}")
    af = ",".join(af)
    cmd = ["ffmpeg", "-y", "-v", "error", "-i", src,
           "-vf", vf, "-af", af,
           "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "14",
           "-preset", "fast", "-c:a", "aac", "-b:a", "192k", dest]
    p = subprocess.run(cmd, capture_output=True, text=True,
                       timeout=1800)
    if p.returncode != 0:
        # stderr to the worker log only (torture-fix F2).
        print(f"index-fallback segment render failed: "
              f"{p.stderr[-500:]}")
        raise ExportError(
            "We couldn't render part of this cut. "
            "Please try again.")
    return dest


def segment_frames_or_fallback(src, clip, seg, ow, oh, tb,
                               fps_num, fps_den, jid):
    """The rendered segment's frames, with the broken-timestamps
    rescue: when the pts-trimmed segment holds fewer than half the
    frames the keep's duration implies, the source's timestamps are
    broken in that range - re-render by decode-order index and keep
    whichever attempt produced more frames. On every healthy source
    (constant-rate or ordinary VFR) the counts match the duration
    and the fallback never fires."""
    a, b = float(clip["source_start"]), float(clip["source_end"])
    frames = probe_frame_times(seg)
    expected = (b - a) * fps_num / fps_den
    if len(frames) < max(1, int(expected * 0.5)):
        print(f"job {jid}: segment [{a:.2f},{b:.2f}) holds "
              f"{len(frames)} frames, expected ~{expected:.0f} - "
              f"broken source timestamps; index fallback")
        try:
            render_segment_by_index(src, clip, ow, oh, tb,
                                    fps_num, fps_den, seg)
            frames2 = probe_frame_times(seg)
            if len(frames2) > len(frames):
                frames = frames2
        except ExportError as e:
            print(f"job {jid}: index fallback failed: {e}")
    return frames


def join_export_audio(seg_paths, fade_secs, tmp):
    """Join the segments' audio on the output timeline: acrossfade
    at faded joins, plain concat at hard joins (the _audio_chain
    discipline, over segment files). Returns the joined WAV path
    (pre-loudnorm)."""
    parts = []
    for i in range(len(seg_paths)):
        parts.append(f"[{i}:a]aresample=48000[a{i}]")
    cur = "a0"
    for i in range(1, len(seg_paths)):
        if fade_secs[i - 1] > 0:
            parts.append(
                f"[{cur}][a{i}]acrossfade=d={fade_secs[i - 1]:.6f}"
                f":c1=tri:c2=tri[ax{i}]")
        else:
            parts.append(f"[{cur}][a{i}]concat=n=2:v=0:a=1[ax{i}]")
        cur = f"ax{i}"
    joined = os.path.join(tmp, "joined.wav")
    cmd = ["ffmpeg", "-y", "-v", "error"]
    for pth in seg_paths:
        cmd += ["-i", pth]
    cmd += ["-filter_complex", ";".join(parts), "-map", f"[{cur}]", joined]
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    if p.returncode != 0:
        raise ExportError(
            "We couldn't build the audio for this export. "
            "Please try again.")
    return joined


def join_export_video(seg_paths, seg_frames, fade_secs, fps_num, fps_den,
                      tb, ass_path, audio_path, dest, total):
    """Join the segments' video with the render_from_edl arithmetic:
    chained xfade at faded joins with offsets computed in whole
    frames from the accumulated frame count, hard concat otherwise;
    captions/overlays burned from the ASS file; the pre-built output
    audio muxed in, and the encode capped at the plan's duration with
    -t (the magic-edit rule). Returns the planned output frame count
    for the fail-closed check.

    Every input branch AND every join output passes through
    settb={tb}: the segment files present a 1/15360 stream timebase
    while concat re-bases its output, and an xfade fed two different
    timebases dies with a bare "Invalid argument" (found the hard
    way: an all-xfade chain rendered, the same chain with one hard
    concat before an xfade did not). Normalizing both sides of every
    join is the ffmpeg xfade-chaining discipline, and it is what
    render_from_edl's per-branch settb does for the C5 path."""
    parts = []
    for i in range(len(seg_paths)):
        parts.append(f"[{i}:v]settb={tb}[vin{i}]")
    vcur = "vin0"
    acc_frames = seg_frames[0]
    for i in range(1, len(seg_paths)):
        fade = fade_secs[i - 1]
        if fade > 0:
            ff = fade_frames_from(fade, fps_num, fps_den)
            off = (acc_frames - ff) * fps_den / fps_num
            parts.append(
                f"[{vcur}][vin{i}]xfade=transition=fade"
                f":duration={fade:.6f}:offset={off:.6f},settb={tb}[vx{i}]")
            acc_frames += seg_frames[i] - ff
        else:
            parts.append(
                f"[{vcur}][vin{i}]concat=n=2:v=1:a=0,settb={tb}[vx{i}]")
            acc_frames += seg_frames[i]
        vcur = f"vx{i}"
    if ass_path:
        parts.append(f"[{vcur}]ass={ass_path}[vburn]")
        vcur = "vburn"
    cmd = ["ffmpeg", "-y"]
    for pth in seg_paths:
        cmd += ["-i", pth]
    cmd += ["-i", audio_path,
            "-filter_complex", ";".join(parts),
            "-map", f"[{vcur}]", "-map", f"{len(seg_paths)}:a",
            "-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "18",
            "-preset", "fast", "-c:a", "aac", "-b:a", "160k",
            "-movflags", "+faststart",
            "-t", f"{total:.3f}", dest]
    return cmd, acc_frames


def fail_export(exp, err):
    """Terminal export failure: ONLY the cut_exports row is marked.
    The job (a finished cut) is untouched — an export can always be
    retried, and it never carried a charge to refund."""
    try:
        rest("PATCH", f"cut_exports?id=eq.{exp['id']}",
             {"status": "failed", "error": str(err)[:500],
              "finished_at": "now()"})
    except Exception as e:  # noqa: BLE001
        print(f"export {exp['id']}: could not record failure: {e}")
    print(f"export {exp['id']} FAILED: {err}")


def process_export(exp):
    exp_id = exp["id"]
    jid = exp["job_id"]
    t0 = time.time()
    t_rn = None
    job_row = None
    tmp = tempfile.mkdtemp(dir=os.environ.get("HOOKCUT_WORKDIR", "/tmp"))
    try:
        rows = rest(
            "GET",
            f"hookcut_jobs?id=eq.{jid}"
            "&select=id,user_id,status,source_storage_key,"
            "media_deleted_at,transcript_words")
        if not rows:
            fail_export(exp, "The cut for this export no longer exists.")
            return
        job = rows[0]
        job_row = job
        if job.get("media_deleted_at") or not job.get("source_storage_key"):
            fail_export(
                exp,
                "The original video for this cut is no longer stored, "
                "so it can't be exported again. Originals are kept for "
                "14 days after a cut finishes.")
            return
        timeline = exp.get("timeline") or {}
        settings = exp.get("settings") or {}
        resolution = settings.get("resolution", "source")
        captions_mode = settings.get("captions", CAPTIONS_DEFAULT)
        if not timeline.get("clips"):
            fail_export(exp, "This timeline has no clips to export.")
            return
        rest("PATCH", f"cut_exports?id=eq.{exp_id}",
             {"status": "rendering", "started_at": "now()",
              "progress": 2, "stage": "starting"})
        # Multi-Clip Jobs: download EVERY source file. Each timeline
        # clip renders from its own file (clip.source_clip, default
        # 0) at its LOCAL range - the stitched range minus that
        # clip's offset, offsets from the probed durations. A
        # one-clip job resolves to the single file and a zero
        # offset, exactly the E7 behaviour.
        srcs_meta = job_sources(job)
        src_paths, src_durs = {}, {}
        for k, s in enumerate(srcs_meta):
            p = os.path.join(tmp, f"source{k}.mp4")
            download(s["storage_key"], p)
            src_paths[k] = p
            src_durs[k] = probe_duration(p)
        offsets = clip_offsets([src_durs[k] for k in sorted(src_durs)])
        src = src_paths[0]
        w_px, h_px, tb, fps_num, fps_den = probe_video(src)
        ow, oh = export_dims(w_px, h_px, resolution)
        fps_frac = f"{fps_num}/{fps_den}" if fps_num > 0 else "30/1"
        if fps_num <= 0:
            # No readable frame rate: joins can't be frame-planned,
            # so the export renders with hard joins throughout (the
            # fade_plan fallback rule), at an assumed 30 fps grid.
            tb = "1/30"
            fps_num, fps_den = 30, 1
        spans = clip_spans_export(timeline)
        t_rn = time.time()
        # -- per-clip segments ------------------------------------
        seg_paths, seg_frames, seg_durs = [], [], []
        for i, sp in enumerate(spans):
            dest = os.path.join(tmp, f"seg{i}.mp4")
            clip = sp["clip"]
            k = clip.get("source_clip") or 0
            local = dict(clip)
            local["source_start"] = clip["source_start"] - offsets[k]
            local["source_end"] = clip["source_end"] - offsets[k]
            render_export_segment(
                src_paths[k], local, ow, oh, tb, fps_frac, dest)
            # Broken-timestamps rescue (torture-fix F2): when the
            # pts-trimmed segment's frame count disagrees with the
            # clip's timeline duration, the source's timestamps are
            # broken in that range - re-render by decode-order
            # index (the job render's tier, via the shared helper).
            _n = len(probe_frame_times(dest))
            _exp = ((local["source_end"] - local["source_start"])
                    / (float(local.get("speed") or 1.0))
                    * fps_num / fps_den)
            if abs(_n - _exp) > max(2.0, 0.5 * _exp):
                print(f"export {exp_id}: segment {i} holds {_n} "
                      f"frames, expected ~{_exp:.0f} - broken "
                      f"source timestamps; index fallback")
                render_segment_by_index(
                    src_paths[k], local, ow, oh, tb, fps_num,
                    fps_den, dest)
            seg_paths.append(dest)
            seg_frames.append(len(probe_frame_times(dest)))
            seg_durs.append(probe_duration(dest))
            set_export_progress(
                exp_id, 5 + 33 * (i + 1) / len(spans), "rendering")
        # -- join plan (per-join crossfades, frame-quantized) -----
        joins = timeline.get("joins") or []
        fade_secs = []
        for i in range(len(spans) - 1):
            ms = 0
            if i < len(joins):
                ms = joins[i].get("crossfade_ms") or 0
            ff = int(round(ms / 1000.0 * fps_num / fps_den))
            ok = (ff >= 1
                  and seg_frames[i] > ff and seg_frames[i + 1] > ff
                  and seg_durs[i] >= FADE_MIN_SEG_SEC
                  and seg_durs[i + 1] >= FADE_MIN_SEG_SEC)
            fade_secs.append(ff * fps_den / fps_num if ok else 0.0)
        map_time, _out_starts = export_time_mapper(spans, fade_secs)
        # -- audio: join, then loudnorm when the timeline enables it
        joined = join_export_audio(seg_paths, fade_secs, tmp)
        set_export_progress(exp_id, 45, "rendering")
        if timeline.get("loudnorm_on_export", True):
            audio_path, loud_i, loud_passes = normalize_loudness(
                joined, tmp, "exp")
        else:
            audio_path, loud_i, loud_passes = joined, None, 0
        set_export_progress(exp_id, 52, "rendering")
        # -- captions + overlays ----------------------------------
        ass_path = None
        if captions_mode == "burned" or timeline.get("text_overlays"):
            placed = placed_words_export(
                timeline, job.get("transcript_words"))
            ass_text, n_dialogues = export_ass(
                timeline, placed, ow, oh, map_time,
                include_captions=(captions_mode == "burned"))
            if n_dialogues > 0:
                ass_path = os.path.join(tmp, "export.ass")
                with open(ass_path, "w") as f:
                    f.write(ass_text)
        srt_path = None
        if captions_mode in ("burned", "srt"):
            srt_cues = export_srt_cues(timeline, map_time)
            if srt_cues:
                srt_path = os.path.join(tmp, "export.srt")
                with open(srt_path, "w") as f:
                    f.write(captions_srt(srt_cues))
        # -- video join + mux -------------------------------------
        out = os.path.join(tmp, "export.mp4")
        total = sum(seg_durs) - sum(fade_secs)
        cmd, planned_frames = join_export_video(
            seg_paths, seg_frames, fade_secs, fps_num, fps_den, tb,
            ass_path, audio_path, out, total)
        run_ffmpeg_progress_exp(cmd, total, 55, 92, exp_id, "rendering")
        got_frames = count_output_frames(out)
        if got_frames is not None and abs(got_frames - planned_frames) > 1:
            raise ExportError(
                "The exported video didn't match the edited timeline "
                "and may be incomplete. Please export again.")
        out_dur = probe_duration(out)
        set_export_progress(exp_id, 95, "uploading")
        out_key = f"exports/{exp_id}.mp4"
        upload(out_key, out)
        srt_key = None
        if srt_path:
            srt_key = f"exports/{exp_id}.srt"
            upload(srt_key, srt_path, "text/plain; charset=utf-8")
        # The export replaces the job's current output. Earlier
        # outputs (the C5 render, previous exports) keep their files
        # in storage — they are the versioned outputs, referenced by
        # their own rows/keys.
        #
        # Hook Variants: a variant export (settings.variant) is an
        # ALTERNATE rendering of the same cut — the same video with
        # a different hook attempt up front — never a replacement.
        # It must not move the job's current-output pointer, its
        # captions key, or its output duration; the variant lives
        # only on its own export row.
        if not settings.get("variant"):
            rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
                 {"output_storage_key": out_key,
                  "captions_srt_key": srt_key,
                  "output_seconds": round(out_dur, 2)})
        record_export_usage(exp_id, job_row, {
            "outcome": "ready",
            "render_wall_sec":
                round(time.time() - t_rn, 2) if t_rn else None,
            "processing_wall_sec": round(time.time() - t0, 2)})
        rest("PATCH", f"cut_exports?id=eq.{exp_id}",
             {"status": "ready", "progress": 100, "stage": "done",
              "output_storage_key": out_key, "srt_storage_key": srt_key,
              "output_seconds": round(out_dur, 2),
              "finished_at": "now()"})
        print(f"export {exp_id}: job {jid} -> {out_dur:.1f}s "
              f"({len(spans)} clips, {sum(1 for f in fade_secs if f)} "
              f"faded joins, loudness "
              f"{loud_i if loud_i is not None else 'not normalized'} "
              f"LUFS, {loud_passes} loudnorm pass(es))")
    except ExportError as e:
        record_export_usage(exp_id, job_row, {
            "outcome": "failed",
            "render_wall_sec":
                round(time.time() - t_rn, 2) if t_rn else None,
            "processing_wall_sec": round(time.time() - t0, 2)})
        fail_export(exp, e)
    except Exception as e:  # noqa: BLE001
        record_export_usage(exp_id, job_row, {
            "outcome": "failed",
            "render_wall_sec":
                round(time.time() - t_rn, 2) if t_rn else None,
            "processing_wall_sec": round(time.time() - t0, 2)})
        print(f"export {exp_id} ERROR: {type(e).__name__}: {e}")
        fail_export(
            exp,
            "Something went wrong rendering this export. "
            "Please try again.")
    finally:
        subprocess.run(["rm", "-rf", tmp])


def reclaim_stale_exports():
    """An export left in `rendering` for over 45 minutes belongs to
    a worker that died mid-render: fail it (plainly) so the user can
    export again. Exports never touch the job row, so there is
    nothing else to repair."""
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(minutes=45)
    rows = rest(
        "GET",
        "cut_exports?status=eq.rendering&select=id,started_at"
        "&order=created_at.asc&limit=20")
    if isinstance(rows, dict):
        return
    for row in rows or []:
        started = row.get("started_at")
        if not started:
            continue
        try:
            when = datetime.fromisoformat(
                str(started).replace("Z", "+00:00"))
        except ValueError:
            continue
        if when > cutoff:
            continue
        rest("PATCH", f"cut_exports?id=eq.{row['id']}",
             {"status": "failed",
              "error": "This export timed out before it finished. "
                       "Please export again.",
              "finished_at": "now()"})
        print(f"reclaim: export {row['id']} timed out")


RETENTION_DAYS = 14


def delete_object(path):
    try:
        sb_req("DELETE", f"/storage/v1/object/{BUCKET}/{path}")
    except Exception as e:  # noqa: BLE001
        print(f"cleanup: could not delete {path}: {e}")


def cleanup_expired():
    """C7 retention (briefing): a job's SOURCE file is deleted 14 days
    after the job finishes - finished_at is set when it reaches ready
    or failed. The finished output and its captions are NOT deleted
    here: they remain until the user deletes the job or the account
    closes. Jobs that never finished (abandoned uploads) anchor to
    created_at so their sources cannot leak either. media_deleted_at
    records when the source went away; it gates Restore and re-renders,
    which need the source. Job records are always kept."""
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    rows = rest(
        "GET",
        "hookcut_jobs?media_deleted_at=is.null"
        "&select=id,source_storage_key,created_at,finished_at"
        "&order=created_at.asc&limit=50",
    )
    if isinstance(rows, dict):
        print(f"cleanup: unexpected response: {rows}")
        return
    for row in rows or []:
        anchor = row.get("finished_at") or row.get("created_at")
        if not anchor:
            continue
        try:
            when = datetime.fromisoformat(str(anchor).replace("Z", "+00:00"))
        except ValueError:
            continue
        if when > cutoff:
            continue
        # Multi-Clip Jobs: every source file of the job expires,
        # not just the first - the sources table is the full list.
        keys = set()
        if row.get("source_storage_key"):
            keys.add(row["source_storage_key"])
        srows = rest(
            "GET",
            f"hookcut_job_sources?job_id=eq.{row['id']}"
            "&select=storage_key")
        if isinstance(srows, list):
            for sr in srows:
                if sr.get("storage_key"):
                    keys.add(sr["storage_key"])
        for key in keys:
            delete_object(key)
        rest("PATCH", f"hookcut_jobs?id=eq.{row['id']}",
             {"media_deleted_at": "now()"})
        print(f"cleanup: source expired for job {row['id']}")


# ---------------------------------------------------------------------
# Parallel engines (Oct 27, 2026): atomic queue claiming + stale reclaim.
#
# Several engines (this VM's worker pool and the Render host) drain the
# same queues. A claim is a conditional PATCH: it lands only while the
# row is still queued AND carries no claim stamp, and Postgres
# serializes the competing UPDATEs, so exactly one engine wins; the
# loser gets an empty result back and moves on. The winner's stamp
# (claimed_by / claimed_at) stays on the row as the audit trail of who
# processed it and is cleared only when a row returns to the queue.
# Charging happens at submit, never here: claims and reclaims never
# touch the allowance beyond fail_job's own failure refund.
#
# A heartbeat keeps a claimed row's updated_at moving while its
# engine works (see start_heartbeat): long stages can otherwise
# leave a row untouched for many minutes at a stretch (Oct 26, 2026:
# a real job orphaned in 'rendering' when its runner was killed),
# and the stale reclaimer below keys on exactly that stillness.
# With a beat every minute, stillness means death again.
# ---------------------------------------------------------------------
import socket as _socket

ENGINE_ID = "%s-%s-%d" % (
    os.environ.get("HOOKCUT_ENGINE_KIND", ENGINE_KIND),
    _socket.gethostname(), os.getpid())

STALE_JOB_MINUTES = 30
MAX_JOB_ATTEMPTS = 3


def _claim(table, row_id):
    got = rest("PATCH",
               f"{table}?id=eq.{row_id}&status=eq.queued"
               "&claimed_by=is.null",
               {"claimed_by": ENGINE_ID, "claimed_at": "now()"})
    if isinstance(got, list) and got:
        return got[0]
    return None


def claim_next(table):
    """Claim the oldest claimable row of a queue table, atomically.
    Returns the full claimed row, or None when the queue is empty or
    every candidate was just won by another engine."""
    rows = rest("GET",
                f"{table}?status=eq.queued&select=id"
                "&order=created_at.asc&limit=5")
    if not isinstance(rows, list):
        return None
    for row in rows:
        won = _claim(table, row["id"])
        if won:
            return won
    return None


HEARTBEAT_SECONDS = 60


def start_heartbeat(table, row_id):
    """Beat a claimed row's claim stamp once a minute while this
    engine processes it. The PATCH bumps the row's updated_at (and
    refreshes claimed_at), which is what lets the stale reclaimers
    treat a still row as a dead engine's row. Best-effort: a failed
    beat never fails the unit. Returns a stop() callable."""
    import threading

    stop = threading.Event()

    def beat():
        while not stop.wait(HEARTBEAT_SECONDS):
            try:
                rest("PATCH", f"{table}?id=eq.{row_id}",
                     {"claimed_at": "now()"})
            except Exception:  # noqa: BLE001 - liveness is best-effort
                pass

    threading.Thread(target=beat, daemon=True).start()
    return stop.set


def reclaim_stale_jobs():
    """Self-healing for engine deaths, safe with parallel engines.

    A job left in an active status by an engine that died mid-job
    (host restart, OOM kill) would otherwise sit there forever: only
    queued jobs are ever claimed. A job whose row has not been touched
    in STALE_JOB_MINUTES goes back in the queue; after
    MAX_JOB_ATTEMPTS reclamations it is failed through fail_job, so
    the refund and the cut_failed event match any other failure. The
    rescue writes carry the same staleness condition they were read
    under, so a job that moved in the meantime is never stolen from a
    live engine. Claims whose engine died before processing started
    (row still queued, stamp stale) simply have their stamp released.
    """
    from datetime import datetime, timedelta, timezone
    from urllib.parse import quote

    cutoff = (datetime.now(timezone.utc)
              - timedelta(minutes=STALE_JOB_MINUTES)).isoformat()
    q = quote(cutoff, safe=":")

    rows = rest(
        "GET",
        "hookcut_jobs?status=in.(transcribing,editing,rendering)"
        f"&updated_at=lt.{q}"
        "&select=id,attempts,status,edits_charged,user_id&limit=10")
    if isinstance(rows, list):
        for row in rows or []:
            attempts = (row.get("attempts") or 0) + 1
            if attempts >= MAX_JOB_ATTEMPTS:
                marked = rest(
                    "PATCH",
                    f"hookcut_jobs?id=eq.{row['id']}"
                    f"&status=eq.{row['status']}&updated_at=lt.{q}",
                    {"attempts": attempts})
                if isinstance(marked, list) and marked:
                    err = RuntimeError(
                        "This video could not be cut after several "
                        "tries. Try exporting it at 1080p and upload "
                        "it again.")
                    fail_job(row, err, row.get("status") or "rendering")
                    print(f"reclaim: job {row['id']} failed after "
                          f"{attempts} attempts")
            else:
                got = rest(
                    "PATCH",
                    f"hookcut_jobs?id=eq.{row['id']}"
                    f"&status=eq.{row['status']}&updated_at=lt.{q}",
                    {"status": "queued", "attempts": attempts,
                     "progress": 0, "stage": "queued",
                     "started_at": None,
                     "claimed_by": None, "claimed_at": None})
                if isinstance(got, list) and got:
                    print(f"reclaim: job {row['id']} requeued "
                          f"(attempt {attempts})")

    for table in ("hookcut_jobs", "cut_exports"):
        rows = rest(
            "GET",
            f"{table}?status=eq.queued&claimed_by=not.is.null"
            f"&claimed_at=lt.{q}&select=id&limit=10")
        if isinstance(rows, list):
            for row in rows or []:
                rest("PATCH",
                     f"{table}?id=eq.{row['id']}&claimed_at=lt.{q}",
                     {"claimed_by": None, "claimed_at": None})


def main():
    cleanup_expired()
    reclaim_stale_jobs()
    reclaim_stale_exports()
    reclaim_stale_pickups()
    reclaim_stale_rechecks()
    # E7: exports drain before new cuts — they are quick, user-waiting
    # renders of finished work, and an export never touches the
    # allowance either way. Claims are atomic (see the block above):
    # with several engines running, each unit of work goes to exactly
    # one of them, never two.
    exp = claim_next("cut_exports")
    if exp:
        stop = start_heartbeat("cut_exports", exp["id"])
        try:
            process_export(exp)
        finally:
            stop()
        return
    # The Director's passes sit between exports and new cuts:
    # a pickup transcription or a recheck belongs to a job the
    # member is actively finishing, and neither ever touches
    # the allowance.
    pickup = claim_next_pickup()
    if pickup:
        process_pickup(pickup)
        return
    recheck = claim_next_recheck()
    if recheck:
        process_recheck(recheck)
        return
    job = claim_next("hookcut_jobs")
    if not job:
        return
    stop = start_heartbeat("hookcut_jobs", job["id"])
    try:
        process(job)
    finally:
        stop()


if __name__ == "__main__":
    main()
