from reddit_dfp.lib.dfp import DfpQuery
from reddit_dfp.services.advertisers_service import AdvertisersService

def get_by_user(user):
    advertiser_id = getattr(user, "dfp_advertiser_id", None)

    if not advertiser_id:
        return None

    return AdvertisersService.by_id(advertiser_id)


def create(user):
    data = AdvertisersService.new(user.name, externalId=user._fullname)
    advertiser = AdvertisersService.insert_one(data)

    user.dfp_advertiser_id = advertiser.id
    user._commit()

    return advertiser


def upsert(user):
    advertiser = get_by_user(user)

    if advertiser:
        return advertiser

    return create(user)


def upsert_many(users):
    inserts = filter(lambda user: not getattr(user, "dfp_advertiser_id", False), users)
    existing = filter(lambda user: getattr(user, "dfp_advertiser_id", False), users)

    advertisers = []

    if inserts:
        advertisers += AdvertisersService.insert_many([
            AdvertisersService.new(user.name, externalId=user._fullname)
        for user in inserts])

    if existing:
        query = DfpQuery(
            "WHERE externalId IN (%s)" %
                ", ".join(["'" + user._fullname + "'" for user in existing]),
        )
        advertisers += AdvertisersService.find(query)

    advertisers_by_fullname = {
        advertiser.externalId: advertiser
    for advertiser in advertisers}

    for user in users:
        advertiser = advertisers_by_fullname[user._fullname]
        user.dfp_advertiser_id = advertiser.id
        user._commit()

    return advertisers
