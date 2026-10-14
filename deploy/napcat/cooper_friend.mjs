// NapCat 4.10.41: recover incoming requests hidden by the isDecide/isUnread filter.
let restore;

export function plugin_init(core, obContext, actions) {
  const get = actions.get;
  const Base = Object.getPrototypeOf(get('set_friend_add_request').constructor);

  class CompleteFriendRequest extends Base {
    actionName = 'cooper_complete_friend_request';

    async _handle(payload) {
      const userId = String(payload?.user_id ?? '');
      if (!/^[1-9][0-9]*$/.test(userId) || userId === String(core.selfInfo.uin)) {
        throw new Error('Invalid friend request user_id');
      }
      const uid = await core.apis.UserApi.getUidByUinV2(userId);
      if (!uid) throw new Error('Friend request user not found');
      const buddy = core.context.session.getBuddyService();
      if (buddy.isBuddy(uid)) return { is_friend: true };

      const requests = (await core.apis.FriendApi.getBuddyReq()).buddyReqs;
      // 1=pending, 2=accepted without adding, 9=allow anyone, 11=add failed.
      // Never revive rejected/ignored requests or act on an outgoing request.
      const request = requests.filter(r => r.friendUid === uid)
        .sort((a, b) => Number(b.reqTime) - Number(a.reqTime))[0];
      if (!request?.reqTime || request.isInitiator || ![1, 2, 9, 11].includes(request.reqType)) {
        return { is_friend: false };
      }

      await buddy.approvalFriendRequest({ friendUid: uid, reqTime: request.reqTime, accept: true });
      // Native approval can return before the local friend list is updated.
      for (let attempt = 0; attempt < 6; attempt++) {
        if (buddy.isBuddy(uid)) {
          if (payload.remark) await core.apis.FriendApi.setBuddyRemark(uid, String(payload.remark));
          return { is_friend: true };
        }
        await new Promise(resolve => setTimeout(resolve, 500));
      }
      return { is_friend: false };
    }
  }

  const action = new CompleteFriendRequest(obContext, core);
  const lookup = key => key === action.actionName ? action : get(key);
  actions.get = lookup;
  restore = () => { if (actions.get === lookup) actions.get = get; };
}

export function plugin_cleanup() {
  restore?.();
  restore = undefined;
}
