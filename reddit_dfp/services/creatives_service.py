from googleads import dfp
from pylons import g

from r2.lib.utils import to36
from r2.models import (
    Account,
    Link,
    promo,
)

from reddit_dfp.lib import utils
from reddit_dfp.lib.dfp import DfpService
from reddit_dfp.lib.merge import merge_deep
from reddit_dfp.services import (
    advertisers_service,
)

NATIVE_SIZE = {
    "width": "1",
    "height": "1",
}

class CreativesServices(ServiceWrapper):
    _singular_name = "Creative"
    _plural_name = "Creatives"

    @classmethod
    def new(cls, creative_type, size=NATIVE_SIZE, advertiser=None, **kwargs):
        obj = merge.merge({}, kwargs, {
            "xsi_type": creative_type,
            "size": size,
        })

        if advertiser:
            obj["advertiserId"] = advertiser.id

        return obj
