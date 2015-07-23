import json

from collections import defaultdict
from datetime import datetime, timedelta
from pylons import g

from r2.config import feature
from r2.lib import (
    amqp,
)

from reddit_dfp.lib.errors import RateLimitException

DFP_QUEUE = "dfp_q"
RATE_LIMIT_ENDS_AT = "dfp-rate-limit-ends-at"


class Processor():
    def __init__(self):
        self._handlers = defaultdict(list)

    def get_handlers(self, action):
        return self._handlers[action]

    def call(self, action, *args, **kwargs):
        handlers = self.get_handlers(action)
        results = []

        for handler in handlers:
            results.append(handler(*args, **kwargs))

        return results

    def register(self, action, handler):
        existing = self.get_handlers(action)
        existing.append(handler)


class Queue():
    def __init__(self, name, **options):
        self.name = self._get_fq_name(name)
        self.options = options

    def push(self, payload):
        message = json.dumps(payload)
        amqp.add_item(self.name, message)
        g.log.debug("%s: queued message: \"%s\"" % (self.name, message))

    @staticmethod
    def _get_fq_name(name):
        return "%s_q" % name


class QueueManager():
    def __init__(self):
        self.queues = {};

    def add(self, name, **options):
        self.queues[name] = Queue(name, **options)

    def get(self, name):
        return getattr(self.queues, name, None)

    def register_all(self, queues):
        from r2.config.queues import MessageQueue

        queues.declare({
            queue.name: MessageQueue(bind_to_self=True, **queue.options)
        for queue in self.queues.itervalues()})


def process_advertisers(limit=10):
    from r2.models import (
        Thing,
    )

    from reddit_dfp.services import (
        advertisers_service,
    )

    @g.stats.amqp_processor("advertisers_q")
    def _process(messages, channel):
        fullnames = []
        callbacks = []

        for message in messages:
            data = json.loads(message.body)

            fullnames.append(data["user"])

            if hasattr(data, "next"):
                callbacks.append(data["next"])

        users = Thing._by_fullname(
            fullnames,
            data=True,
            return_dict=False,
        )

        advertisers_service.bulk_upsert(users)

        for callback in callbacks:
            g.dfp_queue_manager.get(
                callback["queue"]).push(
                    callback["payload"])


    amqp.handle_items("advertisers_q", _process, limit=limit)


def process_creatives(limit=10):
    from r2.models import (
        Thing,
    )

    from reddit_dfp.services import (
        creatives_service,
    )

    @g.stats.amqp_processor("creatives_q")
    def _process(messages, channel):
        fullnames = []
        callbacks = []

        for message in messages:
            data = json.loads(message.body)

            fullnames.append(data["user"])

            if hasattr(data, "next"):
                callbacks.append(data["next"])

        links = Thing._by_fullname(
            fullnames,
            data=True,
            return_dict=False,
        )

        creatives_service.bulk_upsert(links)

        for callback in callbacks:
            g.dfp_queue_manager.get(
                callback["queue"]).push(
                    callback["payload"])


    amqp.handle_items("creatives_q", _process, limit=limit)


def process_orders(limit=10):
    from r2.models import (
        Thing,
    )

    from reddit_dfp.services import (
        orders_service,
    )

    @g.stats.amqp_processor("orders_q")
    def _process(messages, channel):
        fullnames = []
        inserts = []
        approvals = []
        rejections = []
        callbacks = []

        for message in messages:
            data = json.loads(message.body)
            action = data["action"]
            fullname = data["link"]

            fullnames.append(fullname)

            if action == "insert":
                inserts.append(fullname)
            elif action == "approve":
                approvals.append(fullname)
            elif action == "reject":
                rejections.append(fullname)

            if hasattr(data, "next"):
                callbacks.append(data["next"])

        links = Thing._by_fullname(
            fullnames,
            data=True,
            return_dict=False,
        )

        orders_service.bulk_insert(filter(lambda link: link._fullname in inserts, links))
        orders_service.approve(filter(lambda link: link._fullname in approvals, links))
        orders_service.reject(filter(lambda link: link._fullname in rejections, links))

        for callback in callbacks:
            g.dfp_queue_manager.get(
                callback["queue"]).push(
                    callback["payload"])


    amqp.handle_items("orders_q", _process, limit=limit)


def process():
    from r2.models import (
        Account,
        Link,
        NotFound,
        PromoCampaign,
    )

    from reddit_dfp.services import (
        creatives_service,
        lineitems_service,
    )

    def _handle_upsert_promotion(payload):
        link = Link._by_fullname(payload["link"], data=True)
        author = Account._byID(link.author_id)

        creatives_service.upsert_creative(author, link)


    def _handle_upsert_campaign(payload):
        link = Link._by_fullname(payload["link"], data=True)
        campaign = PromoCampaign._by_fullname(payload["campaign"], data=True)
        owner = Account._byID(campaign.owner_id)
        author = Account._byID(link.author_id)

        try:
            lineitem = lineitems_service.upsert_lineitem(owner, campaign)
        except ValueError as e:
            g.log.error("unable to upsert lineitem: %s" % e)
            return

        creative = creatives_service.upsert_creative(author, link)

        lineitems_service.associate_with_creative(
            lineitem=lineitem, creative=creative)


    def _handle_deactivate(payload):
        campaign_ids = payload["campaigns"] and payload["campaigns"].split(",")

        if not campaign_ids:
            return

        campaigns = PromoCampaign._by_fullname(campaign_ids, data=True)
        lineitems_service.deactivate(campaign_ids)


    def _handle_activate(payload):
        campaign_ids = payload["campaigns"] and payload["campaigns"].split(",")

        if not campaign_ids:
            return

        campaigns = PromoCampaign._by_fullname(campaign_ids, data=True)

        lineitems_service.activate(campaign_ids)


    def _handle_check_edits(payload):
        existing = Link._by_fullname(payload["link"], data=True)
        creative = creatives_service.get_creative(existing)

        link = utils.dfp_creative_to_link(
            creative, link=Link._by_fullname(payload["link"], data=True))

        link.dfp_checking_edits = False
        link._commit()


    processor = Processor()

    if feature.is_enabled("dfp_selfserve"):
        g.log.debug("dfp enabled, registering %s processors", DFP_QUEUE)
        processor.register("upsert_promotion", _handle_upsert_promotion)
        processor.register("upsert_campaign", _handle_upsert_campaign)
        processor.register("activate", _handle_activate)
        processor.register("deactivate", _handle_deactivate)

    processor.register("check_edits", _handle_check_edits)

    @g.stats.amqp_processor(DFP_QUEUE)
    def _handler(message):
        rate_limit_ends_at = g.cache.get(RATE_LIMIT_ENDS_AT)
        now_utc = datetime.utcnow()

        if rate_limit_ends_at:
            if now_utc > rate_limit_ends_at:
                g.cache.delete(RATE_LIMIT_ENDS_AT)
            else:
                raise RateLimitException("waiting until %s" % rate_limit_ends_at)

        data = json.loads(message.body)
        g.log.debug("processing action: %s" % data)

        action = data.get("action")
        payload = data.get("payload")

        try:
            processor.call(action, payload)
        except RateLimitException as e:
            g.cache.set(RATE_LIMIT_ENDS_AT, datetime.utcnow() + timedelta(minutes=1))
            raise e

    amqp.consume_items(DFP_QUEUE, _handler, verbose=False)

