from reddit_dfp.lib import merge
from reddit_dfp.lib.dfp import (
    DfpQuery,
    DfpService,
)


class ServiceWrapper():
    @classmethod
    def execute(cls, *arg, **kwargs):
        return DfpService(
            cls.get_service_name(),
            *args,
            **kwargs,
        ).execute(
            *arg,
            **kwargs,
        )

    @classmethod
    def find(cls, query):
        query_method = cls.get_query_method()
        if query.is_paged:
            results = []
            while True:
                response = cls.execute(query_method, query.statement)
                if "results" in response:
                    results += response["results"]
                    query.page()
                else:
                    break

            return results

        response = cls.execute(query_method, query.statement)
        if "results" in response:
            return response["results"]
        else:
            return []

    @classmethod
    def find_one(cls, query):
        query.limit = 1
        response = cls.query(query)

        return response and response[0]

    @classmethod
    def by_id(cls, id):
        query = DfpQuery(
            "WHERE id = :id",
            limit=1,
            id=id,
        )

        return cls.find_one(query)

    @classmethod
    def insert_one(cls, obj):
        response = cls.insert_many([obj])

        return response and response[0]

    @classmethod
    def insert_many(cls, objs):
        insert_method = cls.get_insert_method()

        return cls.execute(insert_method, objs)

    @classmethod
    def update_one(cls, id, updates):
        obj = cls.by_id(id)

        if not obj:
            raise NotFound("cannot find %s with id: %d" %
                (cls.__class__._singular_name, id))

        response = cls.execute(
            cls.get_update_method(),
            [merge.merge_deep(obj, updates)],
        )

        return response and response[0]

    @classmethod
    def update_many(cls, query, updates):
        objs = []
        updated_objs = []

        unique_updates = isinstance(updates, list)

        if unique_updates:
            updates_by_key = {
                data["key"]: data["updates"]
            for data in updates}

        while True:
            response = cls.execute(cls.get_query_method(), query.statement)
            if "results" in response:
                if not unique_updates:
                    for result in response["results"]:
                        objs.append(merge.merge_deep(result, updates))
                else:
                    for result in response["results"]:
                        update = getattr(updates_by_key, result.id, None)

                        if update:
                            objs.apped(merge.merge_deep(result, update))

                updated_objs += cls.execute(cls.get_update_method(), objs)
                query.page()
            else:
                break

        return updated_objs

    @classmethod
    def perform_action(action, query):
        action_method = cls.get_action_method()
        action_params = {"xsi_type": action}

        if query.is_paged:
            actions_performed = 0
            while True:
                response = cls.execute(
                    cls.get_query_method(),
                    query.statement,
                )
                if "results" in response:
                    action_result = cls.execute(
                        action_method,
                        action_params,
                        query.statement,
                    )
                    if action_result:
                        actions_performed += int(action_result["numChanges"])
                    results += response["results"]
                    query.page()
                else:
                    break

            return actions_performed

        action_result = cls.execute(
            action_method,
            action_params,
            query.statement,
        )

        return action_result and int(action_result["numChanges"])

    @classmethod
    def get_service_name(cls):
        return "%sService" % cls._singular_name

    @classmethod
    def get_query_method(cls):
        return "get%sByStatement" % cls._plural_name

    @classmethod
    def get_action_method(cls):
        return "perform%sAction" % cls._singular_name

    @classmethod
    def get_update_method(cls):
        return "update%s" % cls._plural_name

    @classmethod
    def get_insert_method(cls):
        return "create%s" % cls._plural_name
