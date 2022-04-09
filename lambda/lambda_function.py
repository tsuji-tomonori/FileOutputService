import os
from typing import NamedTuple

from aws_resource import S3
from input_io import event_to_message
from input_io import MessageIoTuple


class ChatItem(NamedTuple):
    # メタ情報
    meta_type: str  # メッセージタイプ
    meta_publishedat: str  # メッセージが最初に公開された日時 ISO8601
    # メッセージ情報
    message_text: str  # メッセージ
    message_channelid: str  # メッセージを作成したユーザーのID
    # 削除情報
    deleted_messageid: str  # 削除されたメッセージを一意に識別するID
    # 無料スパチャ情報
    member_usercomment: str  # コメント
    member_month: int  # メンバー合計月数(切り上げ)
    member_lebelname: str  # メンバーレベル名
    # 新規メンバー情報
    newsponsor_lebelname: str  # メンバーレベル名
    newsponsor_upgrade: bool  # アップグレードの有無 新規メンバーはfalse
    # スパチャ情報
    superchat_amountmicros: float  # 金額(マイクロ単位)
    superchat_currency: str  # 通貨(ISO4217)
    superchat_usercomment: str  # コメント
    superchat_tier: int  # 有料メッセージの階級
    # スーパーチケット情報
    supersticker_id: str  # ステッカーを一意に識別するID
    supersticker_alttext: str  # ステッカーを説明する文字列
    supersticker_amountmicros: float  # 金額(マイクロ単位)
    supersticker_currency: str  # 通貨(ISO4217)
    supersticker_tier: int  # 有料メッセージの階級
    # メンバーギフト情報(送る側)
    membergift_count: int  # ユーザーが購入したメンバーシップギフトの数
    membergift_lebelname: str  # 購入したメンバーシップギフトのレベル
    # メンバーギフト情報(受け取る側)
    membergiftreceive_lebelname: str  # 受け取ったメンバーシップギフトのレベル
    # ユーザー情報
    author_channel_id: str  # チャンネルID
    author_display_name: str  # 表示名
    author_is_verified: bool  # YouTubeに確認されているか否か
    author_is_chatowner: bool  # ライブチャットの所有者か否か
    author_is_chatsponsor: bool  # メンバーシップに入っているか否か
    author_is_chatmoderator: bool  # ライブチャットのモデレーターか否か
    # ban情報
    ban_channelid: str  # banされたユーザーのチャンネルID
    ban_display_name: str  # banされたユーザーのチャンネル表示名


def handler(event, context):
    input_bucket = os.environ["INPUT_BUCKET_NAME"]
    output_bucket = os.environ["OUTPUT_BUCKET_NAME"]
    for record in event["Records"]:
        service(event_to_message(record), input_bucket, output_bucket)


def service(input_io: MessageIoTuple, input_bucket: str, output_bucket: str) -> None:

    s3 = S3()

    chat_items = [
        chat_item for path in s3.list_paths(input_bucket, f"{input_io.channel_id}/{input_io.video_id}") for chat_item in read_chatitems(input_bucket, path, s3)]
    s3.upload(
        data=to_csv(ChatItem._fields, chat_items),
        bucket_name=output_bucket,
        file_path=f"{input_io.channel_id}/{input_io.video_id}.csv",
        tags={
            "channel_id": input_io.channel_id,
            "video_id": input_io.video_id,
            "creater": "sdk",
            "project": "FileOutputService",
        }
    )


def read_chatitems(bucket: str, path: str, s3: S3) -> list:
    return [list(dict_to_chatitem(item)) for item in s3.read_file(bucket, path)["items"]]


def get_by_path(target: dict, paths: list) -> object:
    result = target
    for i, path in enumerate(paths):
        if i is not len(paths) - 1:
            result = result.get(path, {})
        else:
            result = result.get(path, "")
    return result


def dict_to_chatitem(action: dict) -> ChatItem:
    snippet = action["snippet"]
    author = action["authorDetails"]
    return ChatItem(
        meta_type=snippet["type"],
        meta_publishedat=snippet["publishedAt"],
        message_text=get_by_path(
            snippet, ["textMessageDetails", "messageText"]),
        message_channelid=get_by_path(snippet, ["authorChannelId"]),
        deleted_messageid=get_by_path(
            snippet, ["messageDeletedDetails", "deletedMessageId"]),
        member_usercomment=get_by_path(
            snippet, ["memberMilestoneChatDetails", "userComment"]),
        member_month=get_by_path(
            snippet, ["memberMilestoneChatDetails", "memberMonth"]),
        member_lebelname=get_by_path(
            snippet, ["memberMilestoneChatDetails", "memberLevelName"]),
        newsponsor_lebelname=get_by_path(
            snippet, ["newSponsorDetails", "memberLevelName"]),
        newsponsor_upgrade=get_by_path(
            snippet, ["newSponsorDetails", "isUpgrade"]),
        superchat_amountmicros=get_by_path(
            snippet, ["superChatDetails", "amountMicros"]),
        superchat_currency=get_by_path(
            snippet, ["superChatDetails", "currency"]),
        superchat_usercomment=get_by_path(
            snippet, ["superChatDetails", "userComment"]),
        superchat_tier=get_by_path(snippet, ["superChatDetails", "tier"]),
        supersticker_id=get_by_path(
            snippet, ["superStickerDetails", "superStickerMetadata", "stickerId"]),
        supersticker_alttext=get_by_path(
            snippet, ["superStickerDetails", "superStickerMetadata", "altText"]),
        supersticker_amountmicros=get_by_path(
            snippet, ["superStickerDetails", "amountMicros"]),
        supersticker_currency=get_by_path(
            snippet, ["superStickerDetails", "currency"]),
        supersticker_tier=get_by_path(
            snippet, ["superStickerDetails", "tier"]),
        membergift_count=get_by_path(
            snippet, ["membershipGiftingDetails", "giftMembershipsCount"]),
        membergift_lebelname=get_by_path(
            snippet, ["membershipGiftingDetails", "giftMembershipsLevelName"]),
        membergiftreceive_lebelname=get_by_path(
            snippet, ["giftMembershipReceivedDetails", "memberLevelName"]),
        author_channel_id=author["channelId"],
        author_display_name=author["displayName"],
        author_is_verified=author["isVerified"],
        author_is_chatowner=author["isChatOwner"],
        author_is_chatsponsor=author["isChatSponsor"],
        author_is_chatmoderator=author["isChatModerator"],
        ban_channelid=get_by_path(
            snippet, ["userBannedDetails", "bannedUserDetails", "channelId"]),
        ban_display_name=get_by_path(
            snippet, ["userBannedDetails", "bannedUserDetails", "displayName"]),
    )


def get_action(action: dict) -> dict:
    res = action.get("addChatItemAction", action.get(
        "addLiveChatTickerItemAction"))
    if res is None:
        raise KeyError("Not add Chat Action.")
    return res


def to_csv(header: list, data: list) -> str:
    return "\n".join(",".join(str(l) for l in line) for line in [header] + data)
