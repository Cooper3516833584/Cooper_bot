import assert from 'node:assert/strict';
import { plugin_init, plugin_cleanup } from '../../deploy/napcat/cooper_friend.mjs';

class Base {
  constructor(ob, core) { this.core = core; }
}
class StandardAction extends Base {}

async function run({ requests = [], isFriend = false, approveWorks = true } = {}) {
  let approvals = 0;
  let remarks = 0;
  const buddy = {
    isBuddy: () => isFriend,
    approvalFriendRequest: async p => {
      assert.equal(p.friendUid, 'target');
      assert.equal(p.accept, true);
      approvals++;
      if (approveWorks) isFriend = true;
    },
  };
  const core = {
    selfInfo: { uin: '99999' },
    context: { session: { getBuddyService: () => buddy } },
    apis: {
      UserApi: { getUidByUinV2: async () => 'target' },
      FriendApi: { getBuddyReq: async () => ({ buddyReqs: requests }), setBuddyRemark: async () => remarks++ },
    },
  };
  const get = () => new StandardAction();
  const actions = { get };
  plugin_init(core, {}, actions);
  try {
    const action = actions.get('cooper_complete_friend_request');
    assert.ok(actions.get('get_friend_list') instanceof StandardAction);
    await assert.rejects(action._handle({ user_id: '0' }));
    await assert.rejects(action._handle({ user_id: '99999' }));
    const result = await action._handle({ user_id: 12345, remark: 'test' });
    return { result, approvals, remarks };
  } finally {
    plugin_cleanup();
    assert.equal(actions.get, get);
  }
}

const incoming = { friendUid: 'target', reqTime: '10', reqType: 9, isInitiator: false, isDecide: true, isUnread: false };
assert.deepEqual(await run({ requests: [incoming] }), { result: { is_friend: true }, approvals: 1, remarks: 1 });
assert.deepEqual(await run({ isFriend: true, requests: [incoming] }), { result: { is_friend: true }, approvals: 0, remarks: 0 });
for (const requests of [[], [{ ...incoming, friendUid: 'other' }], [{ ...incoming, isInitiator: true }],
  [incoming, { ...incoming, reqTime: '11', reqType: 7 }]]) {
  assert.deepEqual(await run({ requests }), { result: { is_friend: false }, approvals: 0, remarks: 0 });
}
assert.deepEqual(await run({ requests: [incoming], approveWorks: false }), { result: { is_friend: false }, approvals: 1, remarks: 0 });
console.log('NapCat friend request tests passed');
