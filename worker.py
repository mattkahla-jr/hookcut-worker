#!/usr/bin/env python3
"""HookCut render worker (host build) - v2 engine — adds live progress reporting.

Same engine as worker.py (silence-cut, single pass, loudnorm), but the job
row now carries progress (0-100) + stage so the web app can show a real
progress bar on the job card and in the top bar:

  2   downloading (fetching the raw upload)
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

ENV_PATH = os.environ.get("HOOKCUT_ENV_FILE", "")
BUCKET = "hookcut"


def load_env():
    env = {}
    if ENV_PATH and os.path.exists(ENV_PATH):
        for line in open(ENV_PATH):
            line = line.strip()
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip().strip('"').strip("'")
    for k in ("NEXT_PUBLIC_SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"):
        if os.environ.get(k):
            env[k] = os.environ[k]
    return env


ENV = load_env()
SB_URL = ENV["NEXT_PUBLIC_SUPABASE_URL"]
SB_KEY = ENV["SUPABASE_SERVICE_ROLE_KEY"]


def sb_req(method, path, data=None, headers_extra=None, raw=False):
    """All Supabase traffic goes through curl: Python urllib reads get
    truncated by this VM's egress proxy (IncompleteRead)."""
    url = f"{SB_URL}{path}"
    cmd = ["curl", "-s", "-X", method, url,
           "-H", f"apikey: {SB_KEY}", "-H", f"Authorization: Bearer {SB_KEY}"]
    infile = None
    if data is not None:
        if isinstance(data, (dict, list)):
            cmd += ["-H", "Content-Type: application/json", "-d", json.dumps(data)]
        else:
            infile = data
            cmd += ["--data-binary", "@-"]
    if headers_extra:
        for k, v in headers_extra.items():
            cmd += ["-H", f"{k}: {v}"]
    p = subprocess.run(cmd, input=infile, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(f"curl failed: {p.stderr.decode()[:200]}")
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
           f"{SB_URL}/storage/v1/object/{BUCKET}/{path}",
           "-H", f"apikey: {SB_KEY}", "-H", f"Authorization: Bearer {SB_KEY}"]
    p = subprocess.run(cmd, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(f"download failed: {p.stderr.decode()[:200]}")


def upload(path, src_file, content_type="video/mp4"):
    # Streamed from disk for the same reason as download().
    cmd = ["curl", "-s", "-f", "-X", "POST",
           f"{SB_URL}/storage/v1/object/{BUCKET}/{path}",
           "-H", f"apikey: {SB_KEY}", "-H", f"Authorization: Bearer {SB_KEY}",
           "-H", f"Content-Type: {content_type}", "-H", "x-upsert: true",
           "--data-binary", f"@{src_file}"]
    p = subprocess.run(cmd, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(f"upload failed: {p.stderr.decode()[:200]}")


def probe_duration(path):
    out = subprocess.check_output(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "csv=p=0", path], text=True)
    return float(out.strip())


def run_ffmpeg_progress(cmd, total_seconds, lo, hi, jid, stage):
    """Run ffmpeg with -progress on stdout; map out_time to [lo, hi].
    stderr goes to a temp file so filter messages (silencedetect) and
    error tails stay readable. Returns the stderr text."""
    err_file = tempfile.NamedTemporaryFile(
        mode="w+", suffix=".log", delete=False, dir=os.environ.get("HOOKCUT_WORKDIR", "/tmp"))
    err_path = err_file.name
    full = cmd[:1] + ["-progress", "pipe:1", "-nostats"] + cmd[1:]
    proc = subprocess.Popen(full, stdout=subprocess.PIPE, stderr=err_file,
                            text=True, bufsize=1)
    last_pct = -1
    for line in proc.stdout:
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
    proc.wait()
    err_file.close()
    with open(err_path) as f:
        err_text = f.read()
    os.unlink(err_path)
    if proc.returncode != 0:
        raise RuntimeError(f"ffmpeg failed: {err_text[-400:]}")
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
           "-preset", "fast", "-c:a", "aac", "-b:a", "160k",
           "-movflags", "+faststart", "-t", f"{total:.3f}", dest]
    run_ffmpeg_progress(cmd, total, 30, 92, jid, "rendering")


def process(job):
    jid = job["id"]
    rest("PATCH", f"hookcut_jobs?id=eq.{jid}",
         {"status": "editing", "started_at": "now()", "progress": 1,
          "stage": "starting"})
    tmp = tempfile.mkdtemp(dir=os.environ.get("HOOKCUT_WORKDIR", "/tmp"))
    src = os.path.join(tmp, "source.mp4")
    out = os.path.join(tmp, "cut.mp4")
    try:
        set_progress(jid, 2, "downloading")
        download(job["source_storage_key"], src)
        if os.path.getsize(src) > 2 * 1024 * 1024 * 1024:
            raise RuntimeError(
                "This file is over the 2 GB limit for Cut. "
                "Please export a smaller file and try again.")
        dur = probe_duration(src)
        if dur > 900:
            raise RuntimeError(
                "This video is over the 15-minute limit for Cut. "
                "Please trim it to 15 minutes or less and try again.")
        # Tight recipe (Matt's call 2026-10-25 after reviewing the first
        # IMG_0081 cut: pacing too loose, tail after he stopped talking
        # survived). More sensitive detector, shorter minimum pause, less
        # padding kept around each cut.
        silences = detect_silences(src, dur, jid, noise="-30dB", min_dur=0.4)
        segments, ncuts = build_segments(dur, silences, pad=0.08, min_gap=0.25)
        rest("PATCH", f"hookcut_jobs?id=eq.{jid}", {"status": "rendering"})
        render(src, segments, out, jid)
        out_dur = probe_duration(out)
        set_progress(jid, 95, "uploading")
        out_path = f"outputs/{jid}.mp4"
        upload(out_path, out)
        rest("PATCH", f"hookcut_jobs?id=eq.{jid}", {
            "status": "ready", "output_storage_key": out_path, "progress": 100,
            "stage": "done",
            "duration_sec": round(dur, 2), "output_seconds": round(out_dur, 2),
            "cuts": ncuts, "finished_at": "now()"})
        print(f"job {jid}: {dur:.1f}s -> {out_dur:.1f}s, {ncuts} silences removed")
    except Exception as e:  # noqa: BLE001
        rest("PATCH", f"hookcut_jobs?id=eq.{jid}", {
            "status": "failed", "error": str(e)[:500], "stage": "error",
            "finished_at": "now()"})
        print(f"job {jid} ERROR: {e}")
    finally:
        subprocess.run(["rm", "-rf", tmp])


RETENTION_DAYS = 7


def delete_object(path):
    try:
        sb_req("DELETE", f"/storage/v1/object/{BUCKET}/{path}")
    except Exception as e:  # noqa: BLE001
        print(f"cleanup: could not delete {path}: {e}")


def cleanup_expired():
    """Media retention: raw uploads and finished cuts are deleted
    RETENTION_DAYS after the job was created. Job records (with the
    before/after stats) are kept; only the video files go."""
    from datetime import datetime, timedelta, timezone
    from urllib.parse import quote

    cutoff = (datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).isoformat()
    rows = rest(
        "GET",
        "hookcut_jobs?media_deleted_at=is.null&created_at=lt."
        + quote(cutoff, safe=":")
        + "&select=id,source_storage_key,output_storage_key&limit=50",
    )
    if isinstance(rows, dict):
        print(f"cleanup: unexpected response: {rows}")
        return
    for row in rows or []:
        if row.get("source_storage_key"):
            delete_object(row["source_storage_key"])
        if row.get("output_storage_key"):
            delete_object(row["output_storage_key"])
        rest("PATCH", f"hookcut_jobs?id=eq.{row['id']}",
             {"media_deleted_at": "now()", "output_storage_key": None})
        print(f"cleanup: media expired for job {row['id']}")


STALE_MINUTES = 12
MAX_ATTEMPTS = 3


def reclaim_stale():
    """Self-healing for worker deaths. If the worker process dies mid-job
    (host restart, OOM kill), the job stays in an active status forever
    because only queued jobs are ever claimed (this orphaned a real job
    twice on 2026-10-25). Any active job whose row has not been touched in
    STALE_MINUTES goes back in the queue; after MAX_ATTEMPTS reclamations
    it is failed with a clear message instead of looping forever. Runs at
    pass start, when this single worker is by definition between jobs, so
    it can never steal a live job."""
    from datetime import datetime, timedelta, timezone
    from urllib.parse import quote

    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=STALE_MINUTES)).isoformat()
    rows = rest(
        "GET",
        "hookcut_jobs?status=in.(editing,rendering)&updated_at=lt."
        + quote(cutoff, safe=":")
        + "&select=id,attempts&limit=10",
    )
    if isinstance(rows, dict):
        print(f"reclaim: unexpected response: {rows}")
        return
    for row in rows or []:
        attempts = (row.get("attempts") or 0) + 1
        if attempts >= MAX_ATTEMPTS:
            rest("PATCH", f"hookcut_jobs?id=eq.{row['id']}", {
                "status": "failed", "attempts": attempts, "stage": "error",
                "error": ("This video could not be cut after several tries. "
                          "Try exporting it at 1080p and upload it again."),
                "finished_at": "now()"})
            print(f"reclaim: job {row['id']} failed after {attempts} attempts")
        else:
            rest("PATCH", f"hookcut_jobs?id=eq.{row['id']}", {
                "status": "queued", "attempts": attempts, "progress": 0,
                "stage": "queued", "started_at": None})
            print(f"reclaim: job {row['id']} requeued (attempt {attempts})")


def main():
    reclaim_stale()
    cleanup_expired()
    jobs = rest("GET", "hookcut_jobs?status=eq.queued&order=created_at.asc&limit=1")
    if not jobs:
        return
    process(jobs[0])


if __name__ == "__main__":
    main()
