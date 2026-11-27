import assert from 'node:assert/strict';
import { plugin_init, plugin_cleanup } from '../../deploy/napcat/cooper_friend.mjs';

class Base {
  constructor(ob, core) { this.core = core; }
}
class StandardAction extends Base {}

async function run({ requests = [], isFriend = false, approveWorks = true, setting = 1,
  settingUin = 12345, sendResult = 0, sendMakesFriend = false, flag, repeated = false } = {}) {
  let approvals = 0;
  let remarks = 0;
  const sends = [];
  const buddy = {
    isBuddy: () => isFriend,
    approvalFriendRequest: async p => {
      assert.equal(p.friendUid, 'target');
      assert.equal(p.accept, true);
      approvals++;
      if (approveWorks) isFriend = true;
    },
    reqToAddFriends: async p => {
      sends.push(p);
      if (sendMakesFriend) isFriend = true;
      return { result: sendResult };
    },
  };
  const core = {
    selfInfo: { uin: '99999' },
    context: { session: { getBuddyService: () => buddy } },
    eventWrapper: {
      callNormalEventV2: async (service, listener, args, checkCall, checkEvent) => {
        const result = { buddyUid: 'target', buddyUin: settingUin, addFriendSetting: setting };
        assert.equal(service, 'NodeIKernelBuddyService/getTargetBuddySetting');
        assert.equal(listener, 'NodeIKernelBuddyListener/onCheckBuddySettingResult');
        assert.deepEqual(args, ['target']);
        assert.equal(checkCall({ result: 0 }), true);
        assert.equal(checkEvent({ buddyUid: 'other' }), false);
        assert.equal(checkEvent(result), true);
        return [{}, result];
      },
    },
    apis: {
      UserApi: { getUidByUinV2: async () => 'target' },
      FriendApi: { getBuddyReq: async () => ({ buddyReqs: requests }), setBuddyRemark: async () => remarks++ },
    },
  };
  const get = () => new StandardAction();
  const actions = { get };
  const realTimeout = globalThis.setTimeout;
  globalThis.setTimeout = callback => queueMicrotask(callback);
  plugin_init(core, {}, actions);
  try {
    const action = actions.get('cooper_complete_friend_request');
    assert.ok(actions.get('get_friend_list') instanceof StandardAction);
    await assert.rejects(action._handle({ user_id: '0' }));
    await assert.rejects(action._handle({ user_id: '99999' }));
    const payload = { user_id: 12345, remark: 'test', flag };
    const operation = action._handle(payload);
    const concurrent = repeated ? action._handle({ ...payload, flag: undefined }) : operation;
    const result = await operation;
    assert.deepEqual(await concurrent, result);
    if (repeated) {
      assert.deepEqual(await action._handle(payload), { is_friend: false, reciprocal: 'pending' });
    }
    return { result, approvals, remarks, sends };
  } finally {
    globalThis.setTimeout = realTimeout;
    plugin_cleanup();
    assert.equal(actions.get, get);
  }
}

const now = Math.floor(Date.now() / 1000);
const incoming = { friendUid: 'target', reqTime: String(now), reqType: 9, isInitiator: false,
  isDecide: true, isUnread: false, sourceId: 3022 };
assert.deepEqual(await run({ requests: [incoming], flag: incoming.reqTime }),
  { result: { is_friend: true }, approvals: 1, remarks: 1, sends: [] });
assert.deepEqual(await run({ isFriend: true, requests: [incoming] }),
  { result: { is_friend: true }, approvals: 0, remarks: 0, sends: [] });
for (const requests of [[], [{ ...incoming, friendUid: 'other' }], [{ ...incoming, isInitiator: true }],
  [{ ...incoming, isDoubt: true }],
  [incoming, { ...incoming, reqTime: String(now + 1), reqType: 7 }]]) {
  assert.deepEqual(await run({ requests }), { result: { is_friend: false }, approvals: 0, remarks: 0, sends: [] });
}
assert.deepEqual(await run({ requests: [incoming], flag: String(now - 1) }),
  { result: { is_friend: false }, approvals: 0, remarks: 0, sends: [] });
assert.deepEqual(await run({ requests: [{ ...incoming, reqTime: String(now - 601) }], approveWorks: false }),
  { result: { is_friend: false }, approvals: 1, remarks: 0, sends: [] });
assert.deepEqual(await run({ requests: [incoming, { ...incoming, reqTime: String(now - 1), reqType: 13, isInitiator: true }] }),
  { result: { is_friend: false, reciprocal: 'pending' }, approvals: 0, remarks: 0, sends: [] });
for (const options of [{ setting: 3 }, { settingUin: 54321 }]) {
  assert.deepEqual(await run({ requests: [incoming], approveWorks: false, ...options }),
    { result: { is_friend: false, reason: 'verification_required' }, approvals: 1, remarks: 0, sends: [] });
}
const { result, approvals, sends } = await run({ requests: [incoming], approveWorks: false, flag: incoming.reqTime, repeated: true });
assert.deepEqual(result, { is_friend: false, reciprocal: 'sent' });
assert.equal(approvals, 1);
assert.deepEqual(sends, [{ buddyUin: '12345', buddyUid: 'target', phoneNumber: '', addFriendSetting: 1,
  verifyInfo: '你好，我是 Cooper_bot，这是对你好友申请的回加。', remark: 'test', sourceID: 3022,
  sourceSubID: 0, qzoneNotWatch: false, qzoneNotWatched: false, onlyChat: false }]);
const immediate = await run({ requests: [incoming], approveWorks: false, setting: 0, sendMakesFriend: true });
assert.deepEqual(immediate.result, { is_friend: true, reciprocal: 'sent' });
assert.equal(immediate.sends[0].verifyInfo, undefined);
const rejected = await run({ requests: [incoming], approveWorks: false, sendResult: 1, repeated: true });
assert.deepEqual(rejected.result, { is_friend: false, reason: 'request_not_confirmed' });
assert.equal(rejected.sends.length, 1);

const events = [];
let listener;
let removedListener;
const nativeBuddy = {
  addKernelBuddyListener: callback => { listener = callback; return 42; },
  removeKernelBuddyListener: id => { removedListener = id; },
  isBuddy: uid => uid === 'existing',
};
const core = {
  selfInfo: { uin: '99999' },
  context: { session: { getBuddyService: () => nativeBuddy } },
  apis: { UserApi: { getUinByUidV2: async () => '12345' } },
};
const get = () => new StandardAction();
const actions = { get };
plugin_init(core, { networkManager: { emitEvent: async event => events.push(event) } }, actions);
try {
  const fresh = { ...incoming, reqTime: String(Math.floor(Date.now() / 1000)) };
  const buddyReqs = [fresh, fresh, { ...fresh, friendUid: 'existing' },
    { ...fresh, friendUid: 'outgoing', isInitiator: true },
    { ...fresh, friendUid: 'normal', reqType: 1, isDecide: false, isUnread: true },
    { ...fresh, friendUid: 'historical', reqTime: String(Number(fresh.reqTime) - 60) }];
  listener.onBuddyReqChange({ buddyReqs });
  listener.onBuddyReqChange({ buddyReqs });
  await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(events, [{ post_type: 'notice', notice_type: 'cooper_friend_request',
    time: Number(fresh.reqTime), self_id: 99999, user_id: 12345, flag: fresh.reqTime }]);
} finally {
  plugin_cleanup();
}
assert.equal(removedListener, 42);
assert.equal(actions.get, get);
console.log('NapCat friend request tests passed');
