from googleads import dfp
from pylons import g

from r2.lib.utils import to36
from r2.models import (
    Account,
)

from reddit_dfp.lib.dfp import (
    DfpQuery,
    DfpService,
)
from reddit_dfp.services import (
    advertisers_service,
)


def _link_to_order(link, advertiser):
    return {
        "name": "%s [selfserve]" % link.title[:115],
        "advertiserId": advertiser.id,
        "salespersonId": g.dfp_selfserve_salesperson_id,
        "traffickerId": g.dfp_selfserve_trafficker_id,
        "externalOrderId": link._id,
    }

def get_order(link):
    order_id = getattr(link, "dfp_order_id", None)

    if not order_id:
        None

    query = DfpQuery(
        "WHERE externalOrderId = :externalOrderId",
        limit=1,
        externalOrderId=order_id,
    )
    response = DfpService("OrderService").query("getOrdersByStatement", query)

    return response and response[0]


def create_order(link):
    advertiser = advertisers_service.upsert_advertiser(author)

    dfp_order_service = DfpService("OrderService")

    orders = [{
        "name": "%s [selfserve]" % link.title[:115],
        "advertiserId": advertiser.id,
        "salespersonId": g.dfp_selfserve_salesperson_id,
        "traffickerId": g.dfp_selfserve_trafficker_id,
        "externalOrderId": link._id,
    }]

    response = dfp_order_service.execute("createOrders", orders)

    order = response and response[0]

    link.dfp_order_id = order.id
    link._commit()

    return order


def upsert_order(link):
    order = get_order(link)

    if order:
        return order

    return create_order(link)


def bulk_insert(links):
    dfp_order_service = DfpService("OrderService")
    authors = Account._byID([link.author_id for link in links], return_dict=False)
    advertisers = advertisers_service.bulk_upsert(authors)
    advertisers_by_author = {
        advertiser.externalId: advertiser
    for advertiser in advertisers}

    orders = dfp_order_service.execute("createOrders", [
        _link_to_order(
            link=link,
            advertiser=advertisers_by_author[
                Account._fullname_from_id36(to36(link.author_id))
            ],
        )
    for link in links])

    orders_by_id = {
        order.externalOrderId: order
    for order in orders}

    for link in links:
        order = orders_by_id[link._id]
        link.dfp_order_id = order.id
        link._commit()

    return orders


def _perform_order_action(action, query):
    dfp_order_service = DfpService("OrderService")
    statement = dfp.FilterStatement(query)
    response = dfp_order_service.execute(
        "performOrderAction",
        {"xsi_type": action},
        statement.ToStatement())

    return response


def approve(links):
    query = ("WHERE status = 'PENDING_APPROVAL' and externalOrderId IN (%s)" %
        ",".join(["'" + link._id + "'" for link in links]))

    try:
        _perform_order_action(action="ApproveOrders", query=query)
    except:
        return False

    return True


def reject(links):
    query = ("WHERE status IN ('PENDING_APPROVAL', 'APPROVED') and externalOrderId IN (%s)" %
        ",".join(["'" + link._id + "'" for link in links]))

    try:
        _perform_order_action(action="DisapproveOrders", query=query)
    except:
        return False

    return True

