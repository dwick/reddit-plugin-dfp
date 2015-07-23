from pylons import g

from r2.lib.utils import to36
from r2.models import (
    Account,
    Link,
    promo,
)

from reddit_dfp import interactions
from reddit_dfp.lib import (
    merge,
    utils,
)
from reddit_dfp.lib.dfp import DfpService
from reddit_dfp.services import (
    CreativesService,
)


def _get_creative_name(link):
    return "%s [%s]" % (utils.trim(link.title, 150), utils.trim(link.url, 100))


def _link_to_creative(link, advertiser=None):
    return CreativesService.new(
        "TemplateCreative",
        advertiser=advertiser,
        name=_get_creative_name(link),
        creativeTemplateId=g.dfp_selfserve_template_id,
        creativeTemplateVariableValues=[{
            "xsi_type": "StringCreativeTemplateVariableValue",
            "uniqueName": "title",
            "value": link.title
        }, {
            "xsi_type": "StringCreativeTemplateVariableValue",
            "uniqueName": "url",
            "value": link.url,
        }, {
            "xsi_type": "StringCreativeTemplateVariableValue",
            "uniqueName": "selftext",
            "value": link.selftext
        }, {
            "xsi_type": "UrlCreativeTemplateVariableValue",
            "uniqueName": "thumbnail_url",
            "value": getattr(link, "thumbnail_url", ""),
        }, {
            "xsi_type": "UrlCreativeTemplateVariableValue",
            "uniqueName": "mobile_ad_url",
            "value": getattr(link, "mobile_ad_url", ""),
        }, {
            "xsi_type": "UrlCreativeTemplateVariableValue",
            "uniqueName": "third_party_tracking",
            "value": getattr(link, "third_party_tracking", ""),
        }, {
            "xsi_type": "UrlCreativeTemplateVariableValue",
            "uniqueName": "third_party_tracking_2",
            "value": getattr(link, "third_party_tracking_2", ""),
        }, {
            "xsi_type": "StringCreativeTemplateVariableValue",
            "uniqueName": "link_id",
            "value": link._fullname,
        }],
    )


def get_by_link(link):
    creative_id = getattr(link, "dfp_creative_id", None)

    if not creative_id:
        return None

    return CreativesService.by_id(creative_id)

def insert(link):
    author = Account._byID(link.author_id)
    advertiser = interactions.advertisers.upsert(author)

    creative = CreativesService.insert_one(
        _link_to_creative(link, advertiser=advertiser))

    link.dfp_creative_id = creative.id
    link._commit()

    return creative

def upsert(link):
    creative_id = getattr(link, "dfp_creative_id", None)

    if not creative_id:
        return insert(link)

    return update(link)


def update(link):
    creative_id = getattr(link, "dfp_creative_id", None)

    if not creative_id:
        raise ValueError("link must have a dfp_creative_id")

    author = Account._byID(link.author_id)
    advertiser = interactions.advertisers.upsert(author)

    return CreativesService.update_one(
        id=creative_id,
        updates=_link_to_creative(link, advertiser=advertiser),
    )


def upsert_many(links):
    updates = filter(lambda user: getattr(user, "dfp_creative_id", False), links)
    inserts = filter(lambda user: not getattr(user, "dfp_creative_id", False), links)
    authors = Account._byID([link.author_id for link in links], return_dict=False, data=True)
    advertisers = interactions.advertisers.bulk_upsert(authors)
    advertisers_by_author_id = {
        utils.fullname_to_id(advertiser.externalId): advertiser
    for advertiser in advertisers}
    creatives = []

    if updates:
        query = DfpQuery(
            "WHERE id IN (%s)" % 
                ", ".join([str(link.dfp_creative_id) for link in updates]))


        creatives += CreativesService.update_many(query, updates=[{
            "key": link.dfp_creative_id,
            "updates": _link_to_creative(
                link=link,
                advertiser=advertisers_by_author_id[link.author_id],
            ),
        } for link in updates])

    if inserts:
        creatives += CreativesService.insert_many([
            _link_to_creative(
                link=link,
                advertiser=advertisers_by_author[
                    Account._fullname_from_id36(to36(link.author_id))
                ],
            )
        for link in inserts])

        creatives += inserted

    creatives_by_fullname = {
        utils.get_template_variable(creative, "link_id"): creative
    for creative in creatives}

    for link in links:
        creative = creatives_by_fullname[link._fullname]
        link.dfp_creative_id = creative.id
        link._commit()

    return creatives

