from reddit_dfp.services.base import ServiceWrapper
from reddit_dfp.lib.dfp import (
    DfpQuery,
)

class AdvertisersServices(ServiceWrapper):
    _singular_name = "Company"
    _plural_name = "Companies"

    @classmethod
    def new(cls, name, **kwargs):
        return merge.merge({}, kwargs, {
            "name": name,
            "type": "ADVERTISER",
        })
