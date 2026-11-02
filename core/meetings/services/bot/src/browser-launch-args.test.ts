/**
 * Join-browser launch flags — an authenticated join must not launch incognito (#1725).
 *
 * The authenticated lane restores a signed-in Chromium profile from S3 and launches a persistent
 * context on it. The join lane's flag set carries `--incognito`; merged into that launch, Chromium
 * opens an incognito context that ignores the restored profile, and the bot reaches the Meet lobby
 * signed out (`auth_session_missing`). Guest joins keep the join lane's flags as they are.
 *
 * Run: npx tsx src/browser-launch-args.test.ts
 */
import { getJoinBrowserArgs } from '@vexa/join';
import { joinBrowserLaunchArgs } from './capture-bridge.js';

let failed = 0;
const check = (name: string, cond: boolean, detail?: string) => {
  console.log(`  ${cond ? '✅' : '❌'} ${name}${cond || !detail ? '' : ` — ${detail}`}`);
  if (!cond) failed++;
};

const joinArgs = getJoinBrowserArgs();
const guest = joinBrowserLaunchArgs(false);
const authenticated = joinBrowserLaunchArgs(true);

check('precondition: the join lane flag set carries --incognito', joinArgs.includes('--incognito'));
check('guest join keeps --incognito', guest.includes('--incognito'));
check('authenticated join drops --incognito so the restored profile is used',
  !authenticated.includes('--incognito'), JSON.stringify(authenticated));
check('authenticated join keeps every other join-lane flag',
  joinArgs.filter((arg) => arg !== '--incognito').every((arg) => authenticated.includes(arg)));
check('the join-lane flags come after the authenticated base set (later wins in Chromium)',
  guest.slice(-joinArgs.length).join(' ') === joinArgs.join(' '));

console.log(failed === 0 ? '\n✅ browser-launch-args: all green' : `\n❌ browser-launch-args: ${failed} failure(s)`);
if (failed > 0) process.exit(1);
