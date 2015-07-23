import re
import time

from googleads import dfp
from googleads import oauth2
from os import path
from pylons import g
from suds import WebFault

from reddit_dfp.lib import errors

RE_GET_BY_STATEMENT = re.compile(r"^get[A-Z][a-z]+ByStatement$")
KEY_FILE = path.join(path.dirname(path.abspath(__file__)), "../../id_dfp")

_client = None

def load_client():
    global _client

    if not _client:
        oauth2_client = oauth2.GoogleServiceAccountClient(
            oauth2.GetAPIScope("dfp"),
            g.dfp_service_account_email,
            KEY_FILE,
        )

        _client = dfp.DfpClient(oauth2_client, g.dfp_project_id)
        _client.network_code = g.dfp_test_network_code if g.dfp_test_mode else g.dfp_network_code


def get_service(service):
    return _client.GetService(service, version=g.dfp_service_version)


def get_downloader():
    return _client.GetDataDownloader(version=g.dfp_service_version)


def get_xsi_type(value):
    if isinstance(value, (int, long)):
        return "NumberValue"
    else:
        return "TextValue"


class DfpService():
    _cache = {}

    def __init__(self, service_name, max_retries=3, delay_exponent=2):
        service = DfpService._cache.get(service_name)

        if service is None:
            service = get_service(service_name)
            DfpService._cache[service_name] = service

        self.service = service
        self.max_retries = max_retries
        self.delay_exponent = delay_exponent

    def execute(self, method, *args, **kwargs):
        g.log.debug("executing %s with %s" % (method, (",".join([str(arg) for arg in args]) + "," + str(kwargs))))

        timer = g.stats.get_timer("providers.dfp")
        timer.start()

        attempt = 1
        response = None
        call = getattr(self.service, method)
        while response == None and attempt <= self.max_retries:
            try:
                response = call(*args, **kwargs)
            except WebFault as e:
                if errors.get_reason(e) == "EXCEEDED_QUOTA":
                    wait = attempt ** self.delay_exponent
                    g.log.debug("failed attempt %d, retrying in %d seconds." % (attempt, wait))
                    time.sleep(wait)
                    attempt += 1
                else:
                    raise e

        timer.stop()

        if not response and attempt == self.max_retries:
            raise errors.RateLimitException("failed after %d attempts" % attempt)

        g.log.debug("%s returned %s" % (method, str(response)))

        return response


class DfpQuery():
    def __init__(self, query, page_size=dfp.SUGGESTED_PAGE_LIMIT, limit=None, **kwargs):
        values = [{
            "key": key,
            "value": {
                "xsi_type": get_xsi_type(value),
                "value": value,
            },
        } for key, value in kwargs.iteritems()]

        self.page_size = page_size
        self.limit = limit
        self._filter = dfp.FilterStatement(query, values, limit)

    def page(self):
        self._filter.offset += self.page_size

    @property
    def statement(self):
        return self._filter.ToStatement()

    @property
    def is_paged(self):
        return self.limit and self.limit > self.page_size
    

