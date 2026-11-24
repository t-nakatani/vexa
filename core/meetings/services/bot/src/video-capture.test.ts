/**
 * L3 — the video recorder's config and delivery, OFFLINE (no browser, no ffmpeg: `cat` stands in for
 * the encoder, a fake frame source for the CDP screencast). It asserts:
 *   • off unless VEXA_VIDEO_RECORDING=1; defaults 640x360@2fps; bad numbers fall back; odd sizes → even;
 *   • the latest frame is re-sent every tick (a still page still advances the timeline);
 *   • bytes are delivered in seq order as non-final chunks, then exactly one is_final chunk on stop;
 *   • the delivered bytes are exactly what the encoder wrote (nothing lost at the slice boundaries);
 *   • stop is idempotent.
 * Run: npx tsx src/video-capture.test.ts
 */
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { startVideoRecorder, videoCaptureConfigFromEnv, videoFfmpegArgs, type FrameSource } from './video-capture.js';
import type { BotRecordingSink } from './recording.js';

let failed = 0;
const check = (name: string, cond: boolean, detail = '') => {
  console.log(`  ${cond ? '✅' : '❌'} ${name}${cond ? '' : '  — ' + detail}`);
  if (!cond) failed++;
};
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function main(): Promise<void> {
  // ── config ──
  check('off by default', videoCaptureConfigFromEnv({}) === null);
  check('off unless exactly "1"', videoCaptureConfigFromEnv({ VEXA_VIDEO_RECORDING: 'true' }) === null);
  const d = videoCaptureConfigFromEnv({ VEXA_VIDEO_RECORDING: '1' })!;
  check('defaults', d.width === 640 && d.height === 360 && d.fps === 2 && d.timesliceMs === 15000, JSON.stringify(d));
  const o = videoCaptureConfigFromEnv({ VEXA_VIDEO_RECORDING: '1', VEXA_VIDEO_WIDTH: '481', VEXA_VIDEO_FPS: 'x', VEXA_VIDEO_BITRATE_KBPS: '-3' })!;
  check('odd width → even, bad numbers → default', o.width === 482 && o.fps === 2 && o.bitrateKbps === 150, JSON.stringify(o));
  const args = videoFfmpegArgs(d).join(' ');
  check('ffmpeg: JPEG stdin → live VP8 WebM stdout', args.includes('-f image2pipe -framerate 2') && args.includes('-c:v libvpx') && args.includes('-f webm -live 1') && args.endsWith('pipe:1'), args);

  // ── delivery (cat as the encoder: stdout == stdin) ──
  const cfg = { ...d, fps: 20, timesliceMs: 120 };
  const chunks: Array<{ seq: number; isFinal: boolean; bytes: Buffer }> = [];
  const sink: BotRecordingSink = {
    chunk: (_k, seq, isFinal, _f, bytes) => { chunks.push({ seq, isFinal, bytes: Buffer.from(bytes) }); },
    close: () => { /* unused */ },
  };
  let emit: ((b: Buffer) => void) | null = null;
  let frameStops = 0;
  const frames: FrameSource = {
    async start(onFrame) { emit = onFrame; onFrame(Buffer.from('A')); },
    async stop() { frameStops++; },
  };
  const stop = await startVideoRecorder({
    frames, sink, key: 'k', cfg,
    spawnEncoder: () => spawn('cat', [], { stdio: ['pipe', 'pipe', 'pipe'] }) as ChildProcessWithoutNullStreams,
  });
  await sleep(300);
  emit!(Buffer.from('B'));
  await sleep(300);
  await stop();
  await stop();

  const all = Buffer.concat(chunks.map((c) => c.bytes)).toString();
  check('still page: the same frame is re-sent every tick', /^A{3,}/.test(all), all.slice(0, 40));
  check('the new frame replaces it', /^A+B+$/.test(all), all);
  check('roughly fps × seconds frames', all.length >= 8 && all.length <= 16, String(all.length));
  check('several chunks, seq 0..n in order', chunks.length >= 3 && chunks.every((c, i) => c.seq === i), chunks.map((c) => c.seq).join(','));
  check('exactly one is_final, and it is last', chunks.filter((c) => c.isFinal).length === 1 && chunks[chunks.length - 1].isFinal);
  check('non-final chunks are never empty', chunks.filter((c) => !c.isFinal).every((c) => c.bytes.length > 0));
  check('frame source stopped once', frameStops === 1, String(frameStops));

  // ── a frame source whose stop never answers (page gone) does not keep the encoder open ──
  const chunks2: Array<{ isFinal: boolean }> = [];
  const stop2 = await startVideoRecorder({
    frames: { async start(onFrame) { onFrame(Buffer.from('C')); }, stop: () => new Promise<void>(() => { /* never */ }) },
    sink: { chunk: (_k, _s, isFinal) => { chunks2.push({ isFinal }); }, close: () => { /* unused */ } },
    key: 'k2', cfg,
    spawnEncoder: () => spawn('cat', [], { stdio: ['pipe', 'pipe', 'pipe'] }) as ChildProcessWithoutNullStreams,
  });
  await sleep(100);
  const t0 = Date.now();
  const settled = await Promise.race([stop2().then(() => true), sleep(6000).then(() => false)]);
  check('hung frame source: stop still settles within the ceiling', settled && Date.now() - t0 < 5000, `settled=${settled} ms=${Date.now() - t0}`);
  check('hung frame source: the final chunk is still delivered', chunks2.filter((c) => c.isFinal).length === 1);

  if (failed) { console.error(`\n❌ ${failed} check(s) failed`); process.exit(1); }
  console.log('\n✅ video-capture: all checks passed');
}

main().catch((e) => { console.error(e); process.exit(1); });
