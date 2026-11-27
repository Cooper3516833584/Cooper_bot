// NapCat 4.10.41 / QQ Linux 3.2.21: complete verified incoming requests.
const REQUEST_MAX_AGE = 10 * 60;
const INCOMING_TYPES = [1, 2, 9, 11];
let restore;

async function waitForBuddy(buddy, uid) {
  for (let attempt = 0; attempt < 6 && !buddy.isBuddy(uid); attempt++) {
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  return buddy.isBuddy(uid);
}

function reportHiddenRequests(core, obContext) {
  const buddy = core.context.session.getBuddyService();
  if (!obContext.networkManager || !buddy.addKernelBuddyListener || !buddy.removeKernelBuddyListener) return;

  const launched = Math.floor(Date.now() / 1000);
  const reported = new Map();
  let listening = true;
  const listener = new Proxy({
    onBuddyReqChange(data) {
      const buddyReqs = data?.buddyReqs;
      if (!Array.isArray(buddyReqs)) return;
      const now = Math.floor(Date.now() / 1000);
      for (const [key, time] of reported) {
        if (time < now - REQUEST_MAX_AGE) reported.delete(key);
      }
      for (const request of buddyReqs) {
        const { friendUid, reqTime, reqType } = request;
        const time = Number(reqTime);
        const key = `${friendUid}:${reqTime}`;
        if (!friendUid || request.isInitiator !== false || request.isDoubt || !INCOMING_TYPES.includes(reqType)
          || (request.isUnread && !request.isDecide) || buddy.isBuddy(friendUid) || reported.has(key)
          || !Number.isSafeInteger(time) || time < Math.max(launched, now - REQUEST_MAX_AGE) || time > now + 30) continue;

        reported.set(key, time);
        void core.apis.UserApi.getUinByUidV2(friendUid).then(uin => {
          const userId = Number(uin);
          if (!listening || !Number.isSafeInteger(userId) || userId <= 0 || String(uin) === String(core.selfInfo.uin)) return;
          return obContext.networkManager.emitEvent({
            post_type: 'notice', notice_type: 'cooper_friend_request',
            time, self_id: Number(core.selfInfo.uin), user_id: userId, flag: String(reqTime),
          });
        }).catch(() => reported.delete(key));
      }
    },
  }, { get: (target, name) => target[name] ?? (() => {}) });
  const id = buddy.addKernelBuddyListener(listener);
  return () => {
    listening = false;
    buddy.removeKernelBuddyListener(id);
  };
}

export function plugin_init(core, obContext, actions) {
  const get = actions.get;
  const Base = Object.getPrototypeOf(get('set_friend_add_request').constructor);
  const active = new Map();
  const attempted = new Map();

  class CompleteFriendRequest extends Base {
    actionName = 'cooper_complete_friend_request';

    async _handle(payload) {
      const userId = String(payload?.user_id ?? '');
      if (!/^[1-9][0-9]*$/.test(userId) || userId === String(core.selfInfo.uin)) {
        throw new Error('Invalid friend request user_id');
      }
      const uid = await core.apis.UserApi.getUidByUinV2(userId);
      if (!uid) throw new Error('Friend request user not found');
      if (active.has(uid)) return active.get(uid);
      const task = this.complete(userId, uid, payload);
      active.set(uid, task);
      try {
        return await task;
      } finally {
        active.delete(uid);
      }
    }

    async complete(userId, uid, payload) {
      const buddy = core.context.session.getBuddyService();
      if (buddy.isBuddy(uid)) return { is_friend: true };

      const { buddyReqs } = await core.apis.FriendApi.getBuddyReq();
      const requests = buddyReqs.filter(r => r.friendUid === uid)
        .sort((a, b) => Number(b.reqTime) - Number(a.reqTime));
      // 1=pending, 2=accepted without adding, 9=allow anyone, 11=add failed.
      // Never revive rejected/ignored requests or act on an outgoing request.
      const request = requests[0];
      if (!request?.reqTime || request.isInitiator !== false || request.isDoubt
        || !INCOMING_TYPES.includes(request.reqType)
        || (payload.flag && String(request.reqTime) !== String(payload.flag))) {
        return { is_friend: false };
      }

      const now = Math.floor(Date.now() / 1000);
      const reqTime = Number(request.reqTime);
      if (!Number.isSafeInteger(reqTime) || reqTime <= 0 || reqTime > now + 30) {
        return { is_friend: false };
      }
      if (requests.some(r => r.isInitiator && [0, 13].includes(r.reqType))) {
        return { is_friend: false, reciprocal: 'pending' };
      }
      for (const [attemptedUid, time] of attempted) {
        if (time < now - REQUEST_MAX_AGE) attempted.delete(attemptedUid);
      }
      if (attempted.get(uid) === reqTime) return { is_friend: false, reciprocal: 'pending' };

      try {
        await buddy.approvalFriendRequest({ friendUid: uid, reqTime: String(request.reqTime), accept: true });
      } catch {
        // QQ may reject another approval of an already decided incoming request.
      }
      if (await waitForBuddy(buddy, uid)) {
        if (payload.remark) await core.apis.FriendApi.setBuddyRemark(uid, String(payload.remark));
        return { is_friend: true };
      }
      if (reqTime < now - REQUEST_MAX_AGE) return { is_friend: false };
      if (typeof buddy.reqToAddFriends !== 'function') {
        return { is_friend: false, reason: 'native_unavailable' };
      }
      let setting;
      try {
        [, setting] = await core.eventWrapper.callNormalEventV2(
          'NodeIKernelBuddyService/getTargetBuddySetting', 'NodeIKernelBuddyListener/onCheckBuddySettingResult',
          [uid], result => result?.result === 0, result => result?.buddyUid === uid,
        );
      } catch {
        return { is_friend: false, reason: 'setting_unavailable' };
      }
      if (String(setting?.buddyUin) !== userId || ![0, 1].includes(setting?.addFriendSetting)) {
        return { is_friend: false, reason: 'verification_required' };
      }
      if (buddy.isBuddy(uid)) return { is_friend: true };

      attempted.set(uid, reqTime);
      try {
        const result = await buddy.reqToAddFriends({
          buddyUin: userId,
          buddyUid: uid,
          phoneNumber: '',
          addFriendSetting: setting.addFriendSetting,
          verifyInfo: setting.addFriendSetting === 1 ? '你好，我是 Cooper_bot，这是对你好友申请的回加。' : undefined,
          remark: payload.remark ? String(payload.remark) : undefined,
          sourceID: Number(request.sourceId) || 3001,
          sourceSubID: 0,
          qzoneNotWatch: false,
          qzoneNotWatched: false,
          onlyChat: false,
        });
        if (result?.result !== 0) {
          return { is_friend: buddy.isBuddy(uid), reason: 'request_not_confirmed' };
        }
      } catch {
        return { is_friend: buddy.isBuddy(uid), reason: 'request_not_confirmed' };
      }
      return { is_friend: await waitForBuddy(buddy, uid), reciprocal: 'sent' };
    }
  }

  const action = new CompleteFriendRequest(obContext, core);
  const lookup = key => key === action.actionName ? action : get(key);
  actions.get = lookup;
  const stopReporting = reportHiddenRequests(core, obContext);
  restore = () => {
    if (actions.get === lookup) actions.get = get;
    stopReporting?.();
  };
}

export function plugin_cleanup() {
  restore?.();
  restore = undefined;
}
