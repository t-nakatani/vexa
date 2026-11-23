/**
 * Low-resolution video of the meeting page, delivered as recording.v1 `video` chunks.
 *
 * ACQUIRE: the page's own compositor frames over CDP `Page.startScreencast` (JPEG, downscaled by the
 * browser). The screencast reads the page, not the X display, so several bots sharing one Xvfb (Lite)
 * each record only their own meeting, whatever window is on top.
 * ENCODE: one ffmpeg per bot turns a constant-rate stream of the latest frame (image2pipe → VP8) into
 * a live WebM on stdout. The latest frame is re-sent on every tick, so a still page still advances
 * the timeline and the video stays in step with the wall clock (and the audio recording).
 * DELIVER: stdout bytes are cut every `timesliceMs` into chunks for the same per-chunk sink the audio
 * recording uses (`createBotRecordingSink({ mediaType: 'video' })`). A live WebM is one continuous
 * byte stream, so the server's byte-concat master of the parts is the whole file.
 *
 * Enabled by VEXA_VIDEO_RECORDING=1 on a recording-enabled bot. Off ⇒ nothing here runs.
 */
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import type { Page } from '@vexa/remote-browser';
import type { BotRecordingSink } from './recording.js';

export interface VideoCaptureConfig {
  width: number;
  height: number;
  fps: number;
  bitrateKbps: number;
  timesliceMs: number;
  jpegQuality: number;
}

const DEFAULTS: VideoCaptureConfig = {
  width: 640, height: 360, fps: 2, bitrateKbps: 150, timesliceMs: 15000, jpegQuality: 60,
};

const NUMERIC_ENV: Array<[keyof VideoCaptureConfig, string]> = [
  ['width', 'VEXA_VIDEO_WIDTH'],
  ['height', 'VEXA_VIDEO_HEIGHT'],
  ['fps', 'VEXA_VIDEO_FPS'],
  ['bitrateKbps', 'VEXA_VIDEO_BITRATE_KBPS'],
  ['timesliceMs', 'VEXA_RECORDING_TIMESLICE_MS'],
  ['jpegQuality', 'VEXA_VIDEO_JPEG_QUALITY'],
];

/** The video config, or null when video recording is off (VEXA_VIDEO_RECORDING is not "1"). Invalid
 *  numbers fall back to the default; width and height are forced even (VP8/yuv420p need it). */
export function videoCaptureConfigFromEnv(env: NodeJS.ProcessEnv = process.env): VideoCaptureConfig | null {
  if (env.VEXA_VIDEO_RECORDING !== '1') return null;
  const cfg: VideoCaptureConfig = { ...DEFAULTS };
  for (const [prop, key] of NUMERIC_ENV) {
    const raw = env[key];
    if (raw === undefined || raw.trim() === '') continue;
    const n = Number(raw);
    if (Number.isFinite(n) && n > 0) cfg[prop] = n;
  }
  cfg.width = Math.max(2, Math.round(cfg.width / 2) * 2);
  cfg.height = Math.max(2, Math.round(cfg.height / 2) * 2);
  cfg.jpegQuality = Math.min(100, Math.round(cfg.jpegQuality));
  return cfg;
}

/** ffmpeg: JPEG frames on stdin at a constant rate → live VP8 WebM on stdout. */
export function videoFfmpegArgs(cfg: VideoCaptureConfig): string[] {
  const { width: w, height: h, fps } = cfg;
  return [
    '-hide_banner', '-loglevel', 'error',
    '-f', 'image2pipe', '-framerate', String(fps), '-c:v', 'mjpeg', '-i', 'pipe:0',
    '-vf', `scale=${w}:${h}:force_original_aspect_ratio=decrease,pad=${w}:${h}:(ow-iw)/2:(oh-ih)/2,format=yuv420p`,
    '-c:v', 'libvpx', '-deadline', 'realtime', '-cpu-used', '8',
    '-b:v', `${cfg.bitrateKbps}k`, '-g', String(Math.max(1, Math.round(fps * 10))),
    '-an', '-f', 'webm', '-live', '1', '-cluster_time_limit', '5000',
    'pipe:1',
  ];
}

/** The page-frame source the recorder needs (CDP in production; a fake in tests). */
export interface FrameSource {
  /** Start delivering frames (JPEG bytes) to `onFrame`. */
  start(onFrame: (jpeg: Buffer) => void): Promise<void>;
  stop(): Promise<void>;
}

/** CDP screencast of `page`, seeded with one screenshot so the video starts at the first tick. */
export function cdpFrameSource(page: Page, cfg: VideoCaptureConfig): FrameSource {
  let cdp: Awaited<ReturnType<ReturnType<Page['context']>['newCDPSession']>> | null = null;
  return {
    async start(onFrame) {
      cdp = await page.context().newCDPSession(page);
      const session = cdp;
      session.on('Page.screencastFrame', (ev: { data: string; sessionId: number }) => {
        onFrame(Buffer.from(ev.data, 'base64'));
        session.send('Page.screencastFrameAck', { sessionId: ev.sessionId }).catch(() => { /* page gone */ });
      });
      try {
        const shot = await session.send('Page.captureScreenshot', {
          format: 'jpeg', quality: cfg.jpegQuality,
        }) as { data: string };
        onFrame(Buffer.from(shot.data, 'base64'));
      } catch { /* the screencast's first frame will seed it */ }
      await session.send('Page.startScreencast', {
        format: 'jpeg', quality: cfg.jpegQuality, maxWidth: cfg.width, maxHeight: cfg.height,
      });
    },
    async stop() {
      if (!cdp) return;
      const session = cdp; cdp = null;
      await session.send('Page.stopScreencast').catch(() => { /* page gone */ });
      await session.detach().catch(() => { /* already detached */ });
    },
  };
}

export interface VideoRecorderDeps {
  frames: FrameSource;
  sink: BotRecordingSink;
  key: string;
  cfg: VideoCaptureConfig;
  log?: (m: string) => void;
  /** Override the encoder process (tests). */
  spawnEncoder?: (args: string[]) => ChildProcessWithoutNullStreams;
}

/**
 * Start recording. Resolves once frames are flowing; returns the stop fn, which ends the encoder,
 * delivers the remaining bytes as the is_final chunk and never throws.
 */
export async function startVideoRecorder(deps: VideoRecorderDeps): Promise<() => Promise<void>> {
  const { frames, sink, key, cfg } = deps;
  const log = deps.log ?? (() => { /* silent */ });
  const encoder = (deps.spawnEncoder ?? ((args) => spawn('ffmpeg', args, { stdio: ['pipe', 'pipe', 'pipe'] })))(videoFfmpegArgs(cfg));

  let latest: Buffer | null = null;
  let writable = true;
  let encoderAlive = true;
  let pending: Buffer[] = [];
  let seq = 0;
  let finalSent = false;
  let totalBytes = 0;

  const exited = new Promise<void>((resolve) => {
    encoder.on('close', (code, signal) => {
      encoderAlive = false;
      if (code !== 0) log(`video: ffmpeg exited code=${code} signal=${signal ?? ''}`);
      resolve();
    });
    encoder.on('error', (e) => { encoderAlive = false; log(`video: ffmpeg failed to start: ${e.message}`); resolve(); });
  });
  encoder.stdin.on('error', () => { writable = false; });
  encoder.stdin.on('drain', () => { writable = true; });
  encoder.stdout.on('data', (b: Buffer) => { pending.push(b); totalBytes += b.length; });
  let stderrTail = '';
  encoder.stderr.on('data', (b: Buffer) => { stderrTail = (stderrTail + b.toString()).slice(-500); });

  const flush = (isFinal: boolean): void => {
    if (finalSent) return;
    const bytes = pending.length ? Buffer.concat(pending) : Buffer.alloc(0);
    pending = [];
    if (!isFinal && bytes.length === 0) return;
    if (isFinal) finalSent = true;
    sink.chunk(key, seq++, isFinal, 'webm', new Uint8Array(bytes));
  };

  const tick = setInterval(() => {
    if (!latest || !encoderAlive || !writable) return;
    writable = encoder.stdin.write(latest);
  }, Math.max(1, Math.round(1000 / cfg.fps)));
  const slicer = setInterval(() => flush(false), cfg.timesliceMs);

  try {
    await frames.start((jpeg) => { latest = jpeg; });
  } catch (e) {
    log(`video: frame source failed to start: ${String(e)}`);
  }
  log(`video: recording ${cfg.width}x${cfg.height}@${cfg.fps}fps ${cfg.bitrateKbps}kbps, chunk every ${cfg.timesliceMs}ms`);

  let stopped: Promise<void> | null = null;
  return () => {
    stopped ??= (async () => {
      clearInterval(tick);
      clearInterval(slicer);
      await frames.stop().catch(() => { /* best-effort */ });
      if (encoderAlive) {
        encoder.stdin.end();
        const timer = setTimeout(() => { encoder.kill('SIGKILL'); }, 10000);
        await exited;
        clearTimeout(timer);
      }
      if (stderrTail.trim()) log(`video: ffmpeg stderr: ${stderrTail.trim()}`);
      flush(true);
      log(`video: stopped after ${seq} chunk(s), ${totalBytes} bytes`);
    })();
    return stopped;
  };
}
