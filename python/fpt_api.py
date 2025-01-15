"""
Thread-safe Flow Production Tracking (FPT) API client with parallel query field processing.

This module provides an enhanced version of the standard FPT API client that:
- Handles query fields efficiently through parallel processing
- Ensures thread-safety for all API operations
- Caches schema information to improve performance
- Uses connection pooling for better resource management

Example:
    >>> fpt = FPT("https://example.fpt.autodesk.com", "script_name", "api_key")
    >>> shots = fpt.find("Shot",
    ...                  filters=[["project", "is", {"type": "Project", "id": 70}]],
    ...                  fields=["code", "sg_query_field"])

Note:
    This implementation requires Python 3.7+ and assumes all requests are done through HTTPS.
"""

from concurrent.futures import ThreadPoolExecutor, Future
import logging
import os
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple, Union, Iterator, Set

import certifi
import urllib3
from requests.packages.urllib3.util.retry import Retry
from shotgun_api3 import Shotgun

logger = logging.getLogger(__name__)


EntityId = int
Entity = Dict[str, Any]
Filters = List[Union[str, List, Dict]]


# Allow switching base class for testing
BaseSG = os.environ.get("FPT_BASE_CLASS", "shotgun_api3.Shotgun")
if BaseSG == "mockgun":
    from shotgun_api3.lib.mockgun import Shotgun as BaseShotgun
else:
    BaseShotgun = Shotgun


class FPT(BaseShotgun):
    """
    Thread-safe FPT client with parallel query field processing.
    """

    def __init__(
        self,
        *args: Any,
        from_handle: Optional[Shotgun] = None,
        timeout_secs: Optional[float] = None,
        connect: bool = True,
        **kwargs: Any
    ) -> None:
        """
        Initialize a new FPT client.

        :param from_handle: Existing FPT instance to copy settings from
        :param timeout_secs: Connection timeout in seconds
        :param connect: Whether to establish connection immediately
        """
        self._schema_cache: Dict[str, Dict] = {}

        # Configure connection parameters
        kparams: Dict[str, Any] = {}
        params = []

        if len(args) == 1 and not from_handle and hasattr(args[0], "find_one"):
            from_handle = args[0]
        else:
            params = args

        if from_handle:
            kparams = {
                "base_url": from_handle.base_url,
                "script_name": from_handle.config.script_name,
                "api_key": from_handle.config.api_key,
                "convert_datetimes_to_utc": from_handle.config.convert_datetimes_to_utc,
                "http_proxy": from_handle.config.raw_http_proxy,
                "login": from_handle.config.user_login,
                "password": from_handle.config.user_password,
                "sudo_as_login": from_handle.config.sudo_as_login,
                "session_token": from_handle.config.session_token,
                "auth_token": from_handle.config.auth_token,
                "ensure_ascii": True,
                "ca_certs": None,
            }
        kparams.update(kwargs)

        # Configure retry strategy for intermittent server errors
        retry_on = [408, 429, 500, 502, 503, 504, 509]
        self._retry_strategy = Retry(
            total=5,
            status_forcelist=retry_on,
            allowed_methods=["GET", "PUT", "POST"],
            backoff_factor=0.1,
        )

        super().__init__(*params, connect=False, **kparams)
        self.config.timeout_secs = timeout_secs
        if connect:
            self.server_caps

    def _http_request(
        self, verb: str, path: str, body: Any, headers: Dict[str, str]
    ) -> Tuple[Tuple[int, str], Dict[str, str], bytes]:
        """
        Make an HTTP request to the FPT server.

        :param verb: HTTP method to use
        :param path: Request path
        :param body: Request body
        :param headers: Request headers
        :returns: Tuple of (status, response headers, response body)
        """
        url = urllib.parse.urlunparse((
            self.config.scheme,
            self.config.server,
            path,
            None,
            None,
            None
        ))
        logger.debug(f"Request: {verb}:{url}")
        logger.debug(f"Headers: {headers}")
        logger.debug(f"Body: {body}")

        conn = self._get_connection()
        resp = conn.request(
            method=verb,
            url=url,
            headers=headers,
            body=body
        )

        http_status = (resp.status, "not supported")
        resp_headers = dict(resp.headers.items())
        resp_body = resp.data

        logger.debug(f"Response status: {http_status}")
        logger.debug(f"Response headers: {resp_headers}")
        logger.debug(f"Response body: {resp_body}")

        return http_status, resp_headers, resp_body

    def _get_connection(self) -> urllib3.PoolManager:
        """
        Get or create a connection pool manager.

        :returns: Connection pool manager.
        """
        if not hasattr(self, "_connection") or self._connection is None:
            self._connection = self._get_urllib3_manager()
        return self._connection

    def _get_urllib3_manager(self) -> urllib3.PoolManager:
        """
        Create a new connection pool manager.

        :returns: Connection pool manager.
        """
        if self.config.proxy_server:
            # Handle proxy authentication
            proxy_headers = None
            if self.config.proxy_user and self.config.proxy_pass:
                auth_string = f"{self.config.proxy_user}:{self.config.proxy_pass}@"
                proxy_headers = urllib3.make_headers(
                    basic_auth=f"{self.config.proxy_user}:{self.config.proxy_pass}"
                )
                proxy_headers["Proxy-Authorization"] = proxy_headers["authorization"]
            else:
                auth_string = ""

            proxy_addr = f"http://{auth_string}{self.config.proxy_server}:{self.config.proxy_port}"

            return urllib3.ProxyManager(
                proxy_addr,
                proxy_headers=proxy_headers,
                timeout=self.config.timeout_secs,
                cert_reqs="CERT_REQUIRED",
                ca_certs=certifi.where(),
                maxsize=10,
                block=True,
                retries=self._retry_strategy,
            )

        return urllib3.PoolManager(
            timeout=self.config.timeout_secs,
            cert_reqs="CERT_REQUIRED",
            ca_certs=certifi.where(),
            maxsize=10,
            block=True,
            retries=self._retry_strategy,
        )

    def find(self, *args: Any, **kwargs: Any) -> List[Entity]:
        """
        Find entities with parallel query field processing.

        :returns: List of Entities found.
        """
        process_query = kwargs.pop("process_query_fields", True)
        if not process_query:
            return super().find(*args, **kwargs)

        # Prepare fields and handle dotted fields
        entity_type, fields, modified_args, modified_kwargs, additional_fields, dotted_query_map = (
            self._prepare_query_fields(args, kwargs)
        )

        if not fields:
            return super().find(*args, **kwargs)

        # Get entities with standard and additional fields
        entities = super().find(*modified_args, **modified_kwargs)
        if not entities:
            return entities

        # Process regular query fields
        entities = self._process_query_fields(entities, modified_args, modified_kwargs)

        # Process dotted query fields and clean up
        if dotted_query_map:
            self._process_dotted_query_fields(entities, dotted_query_map)
            # Remove helper fields
            original_fields = set(fields)
            helper_fields = additional_fields - original_fields
            for entity in entities:
                for field in helper_fields:
                    entity.pop(field, None)

        return entities

    def find_one(self, *args: Any, **kwargs: Any) -> Optional[Entity]:
        """
        Find entities with parallel query field processing.

        :returns: The Entity found or None if not found.
        """
        process_query = kwargs.pop("process_query_fields", True)

        if process_query:
            # Prepare fields and handle dotted fields
            entity_type, fields, modified_args, modified_kwargs, additional_fields, dotted_query_map = (
                self._prepare_query_fields(args, kwargs)
            )
        else:
            modified_args, modified_kwargs = args, kwargs
            fields = additional_fields = dotted_query_map = None

        entity = super().find_one(*modified_args, **modified_kwargs)
        if not entity or not process_query:
            return entity

        # Process regular query fields
        processed = self._process_query_fields([entity], modified_args, modified_kwargs)
        if not processed:
            return None

        result_entity = processed[0]

        # Process dotted query fields and clean up
        if dotted_query_map:
            self._process_dotted_query_fields([result_entity], dotted_query_map)
            # Remove helper fields
            original_fields = set(fields)
            helper_fields = additional_fields - original_fields
            for field in helper_fields:
                result_entity.pop(field, None)

        return result_entity

    def yield_find(self, *args: Any, **kwargs: Any) -> Iterator[Entity]:
        """
        Find entities and yield them one by one as they are processed.

        :yields: Entities found.
        """
        process_query = kwargs.pop("process_query_fields", True)

        # Prepare fields and handle dotted fields
        entity_type, fields, modified_args, modified_kwargs, additional_fields, dotted_query_map = (
            self._prepare_query_fields(args, kwargs)
        )

        if not fields or not process_query:
            yield from super().find(*args, **kwargs)
            return

        # Get entities
        entities = super().find(*modified_args, **modified_kwargs)
        if not entities:
            return

        # Get regular query fields
        if entity_type not in self._schema_cache:
            self._schema_cache[entity_type] = self.schema_field_read(entity_type)
        schema = self._schema_cache[entity_type]

        query_fields = {
            field: schema[field]
            for field in fields
            if '.' not in field and field in schema and "query" in schema[field].get("properties", {})
        }

        # Process entities one by one
        max_workers = min(8, len(query_fields) + len(dotted_query_map))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for entity in entities:
                # Process query fields
                futures, result_entity = self._process_entity_query_fields(
                    entity, query_fields, dotted_query_map, executor
                )

                # Process futures for this entity
                for future, field_name in futures:
                    try:
                        result = future.result(timeout=30)
                        result_entity[field_name] = result
                    except Exception as e:
                        logger.error(f"Error processing query field {field_name}: {e}")
                        result_entity[field_name] = None

                # Remove helper fields
                helper_fields = additional_fields - set(fields)
                for field in helper_fields:
                    result_entity.pop(field, None)

                yield result_entity

    def _find_in_dict(self, obj: Any, target_key: str) -> List[Any]:
        """
        Recursively find all values for a given key in a nested dictionary/list structure.

        :param obj: Object to search through (dict, list, or other)
        :param target_key: Key to find
        :returns: List of found values
        """
        results = []

        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == target_key:
                    results.append(value)
                results.extend(self._find_in_dict(value, target_key))
        elif isinstance(obj, list):
            for item in obj:
                results.extend(self._find_in_dict(item, target_key))

        return results

    def _process_page_filters(self, filters: Dict) -> List[Union[Dict, List]]:
        """
        Process page filters into a format compatible with the FPT API.

        :param filters: Raw page filters
        :returns: List of processed filter conditions
        """
        if not filters or "conditions" not in filters:
            return []

        processed = []
        project_filters = []

        def process_condition(condition: Dict) -> Optional[Union[Dict, List]]:
            # Skip inactive conditions
            if condition.get("active") == "false":
                return None

            # Handle nested conditions
            if "conditions" in condition:
                nested_filters = []
                for nested_condition in condition["conditions"]:
                    # Check for project filter first
                    if nested_condition.get("top_level_project_filter"):
                        project_result = process_condition(nested_condition)
                        if project_result:
                            if isinstance(project_result, list):
                                project_filters.append(project_result)
                            else:
                                project_filters.extend(project_result.get("filters", []))
                        continue

                    nested_result = process_condition(nested_condition)
                    if nested_result:
                        nested_filters.append(nested_result)

                if nested_filters:
                    operator = "any" if condition.get("logical_operator") == "or" else "all"
                    return {
                        "filter_operator": operator,
                        "filters": nested_filters
                    }
                return None

            # Process individual condition
            if all(key in condition for key in ["path", "relation", "values"]):
                # Skip empty values
                if not condition["values"] or condition["values"][0] == "":
                    return None

                value = condition["values"][0]

                # Handle entity references
                if isinstance(value, dict):
                    if value.get("valid") == "parent_entity_token":
                        return None
                    if "type" in value and "id" in value:
                        value = {
                            "type": value["type"],
                            "id": value["id"]
                        }

                # Skip step conditions as they're handled separately
                if condition["path"] == "step":
                    return None

                return [condition["path"], condition["relation"], value]

            return None

        # Process all conditions
        for condition in filters["conditions"]:
            result = process_condition(condition)
            if result:
                processed.append(result)

        # Combine project filters with other filters
        final_filters = []

        # Add project filters first
        if project_filters:
            if len(project_filters) > 1:
                final_filters.append({
                    "filter_operator": "all",
                    "filters": project_filters
                })
            else:
                final_filters.extend(project_filters)

        # Add other filters
        final_filters.extend(processed)

        return final_filters

    def _yield_page_entities(
            self,
            page_id: int,
            additional_filters: Optional[List] = None,
            fields: Optional[List[str]] = None,
    ) -> Iterator[Entity]:
        """
        Yield entities from a page with proper filter processing.

        :param page_id: ID of the page to process
        :param additional_filters: Additional filters to apply
        :param fields: Fields to retrieve
        :yields: Entities found
        """
        # Get page settings
        page = self.find_one("Page", [["id", "is", page_id]], ["id"])
        if not page:
            raise ValueError(f"Page {page_id} not found")

        page_settings = self.find(
            "PageSetting",
            [["page", "is", page]],
            ["settings_json"]
        )

        # Process settings to get entity type and columns
        entity_type = None
        columns = None
        filters = []

        if page_settings:
            # Check first settings for structure and basic info
            first_settings = page_settings[0]["settings_json"]

            # Find entity type - it could be anywhere in the structure
            entity_types = self._find_in_dict(first_settings, "entity_type")
            entity_type = next((et for et in entity_types if et), None)

            # Find columns - could be in different places
            all_columns = self._find_in_dict(first_settings, "columns")
            columns = next((cols for cols in all_columns if isinstance(cols, list)), None)

            # Get filters from both first and last settings
            filter_sources = []

            # Process first settings for initial filters
            filter_sources.extend(self._find_in_dict(first_settings, "filters"))
            filter_sources.extend(self._find_in_dict(first_settings, "panel_filters"))

            # Process last settings for most recent filter state
            if len(page_settings) > 1:
                last_settings = page_settings[-1]["settings_json"]
                filter_sources.extend(self._find_in_dict(last_settings, "filters"))
                filter_sources.extend(self._find_in_dict(last_settings, "panel_filters"))

            # Process each filter source
            for filter_source in filter_sources:
                if isinstance(filter_source, dict):
                    filters.extend(self._process_page_filters(filter_source))

        if not entity_type:
            raise ValueError("Could not determine entity type from page settings")

        # Wrap all filters in an 'all' operator if there are multiple
        if len(filters) > 1:
            final_filters = [{
                "filter_operator": "all",
                "filters": filters
            }]
        else:
            final_filters = filters

        # Add any additional filters
        if additional_filters:
            if final_filters and isinstance(final_filters[0], dict):
                final_filters[0]["filters"].extend(additional_filters)
            else:
                final_filters.extend(additional_filters)

        # Use specified fields or page columns
        query_fields = fields if fields is not None else columns

        # Debug output
        print(f"Entity Type: {entity_type}")
        print(f"Fields: {query_fields}")
        from pprint import pformat
        print(f"Filters: {pformat(final_filters)}")

        # Find and yield entities
        yield from self.find(
            entity_type,
            filters=final_filters,
            fields=query_fields
        )

    def _process_query_fields(
        self, entities: List[Entity], args: Tuple, kwargs: Dict
    ) -> List[Entity]:
        """
        Process query fields for multiple entities in parallel.

        :param entities: List of entities to process
        :param args: Original find arguments
        :param kwargs: Original find keyword arguments
        :returns: Entities with resolved query fields
        """
        # Extract query parameters
        if args:
            entity_type = args[0]
            fields = args[2] if len(args) > 2 else kwargs.get("fields", [])
        else:
            entity_type = kwargs.get("entity_type")
            fields = kwargs.get("fields", [])

        # Use cached schema
        if entity_type not in self._schema_cache:
            self._schema_cache[entity_type] = self.schema_field_read(entity_type)
        schema = self._schema_cache[entity_type]

        # Get query fields
        requested_fields = set(fields)
        query_fields = {
            field: schema[field]
            for field in requested_fields
            if field in schema and "query" in schema[field].get("properties", {})
        }
        if not query_fields:
            return entities

        # Process all entities and fields in parallel
        max_workers = min(8, len(entities) * len(query_fields))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for entity in entities:
                for field_name, field_schema in query_fields.items():
                    future = executor.submit(
                        self._resolve_query_field,
                        field_name=field_name,
                        field_schema=field_schema,
                        parent_entity={"type": entity["type"], "id": entity["id"]}
                    )
                    futures.append((future, entity, field_name))

            # Process results
            result_map = {entity["id"]: entity.copy() for entity in entities}
            for future, entity, field_name in futures:
                result = future.result(timeout=30)
                result_map[entity["id"]][field_name] = result

        return [result_map[entity["id"]] for entity in entities]

    def _process_entity_query_fields(
            self,
            entity: Entity,
            query_fields: Dict[str, Dict],
            dotted_query_map: Dict[str, Tuple[str, str, str]],
            executor: ThreadPoolExecutor
    ) -> Tuple[List[Tuple[Future, str]], Entity]:
        """
        Process query fields for a single entity.

        :returns: A tuple (futures, result_entity)
            where futures is a list of (future, field_name) tuples.
        """
        result_entity = entity.copy()
        futures = []

        # Submit futures for regular query fields
        for field_name, field_schema in query_fields.items():
            future = executor.submit(
                self._resolve_query_field,
                field_name=field_name,
                field_schema=field_schema,
                parent_entity={"type": entity["type"], "id": entity["id"]}
            )
            futures.append((future, field_name))

        # Submit futures for dotted query fields
        for field_name, (linked_type, link_path, query_field) in dotted_query_map.items():
            # Get the linked entity from the path
            linked_entity = result_entity
            for path_part in link_path.split('.'):
                linked_entity = linked_entity.get(path_part, {})
                if not linked_entity:
                    break

            if not linked_entity or not isinstance(linked_entity, dict):
                continue

            # Get schema for the linked entity type
            if linked_type not in self._schema_cache:
                self._schema_cache[linked_type] = self.schema_field_read(linked_type)
            linked_schema = self._schema_cache[linked_type]

            # Get query field schema
            field_schema = linked_schema.get(query_field, {})
            if not field_schema or "query" not in field_schema.get("properties", {}):
                continue

            future = executor.submit(
                self._resolve_query_field,
                field_name=query_field,
                field_schema=field_schema,
                parent_entity={"type": linked_type, "id": linked_entity.get("id")}
            )
            futures.append((future, field_name))

        return futures, result_entity

    def _resolve_query_field(
        self, field_name: str, field_schema: Dict, parent_entity: Dict
    ) -> str:
        """
        Resolve a single query field value.

        :param field_name: Name of the field to resolve
        :param field_schema: Schema definition for the field
        :param parent_entity: Parent entity reference
        :returns: Resolved field value
        """
        properties = field_schema.get("properties", {})
        query = properties.get("query", {}).get("value", {})
        if not query:
            return ""

        entity_type = query.get("entity_type")
        filters = query.get("filters", {}).get("conditions", [])
        processed_filters = self._process_filters(filters, parent_entity)

        summary_type = properties.get("summary_default", {}).get("value")
        summary_field = properties.get("summary_field", {}).get("value")
        summary_value = properties.get("summary_value", {}).get("value", {})
        if summary_type == "single_record":
            return self._handle_record_query(
                entity_type,
                processed_filters,
                summary_field,
                summary_value
            )
        elif summary_type in ["count", "sum", "average", "minimum", "maximum"]:
            return self._handle_aggregate_query(
                entity_type,
                processed_filters,
                summary_field,
                summary_type
            )
        elif summary_type in ["percentage", "status_percentage", "status_percentage_as_float"]:
            return self._handle_percentage_query(
                entity_type,
                processed_filters,
                summary_type,
                summary_field,
                summary_value
            )
        elif summary_type == "record_count":
            return self._handle_count_query(entity_type, processed_filters)

        return ""

    def _prepare_query_fields(
            self,
            args: Tuple,
            kwargs: Dict[str, Any]
    ) -> Tuple[str, List[str], Tuple, Dict[str, Any], Set[str], Dict[str, Tuple[str, str, str]]]:
        """
        Prepare query fields and handle dotted fields.

        :returns: tuple(entity_type, fields, modified_args, modified_kwargs, additional_fields, dotted_query_map)
        """
        # Extract query parameters
        if args:
            entity_type = args[0]
            fields = args[2] if len(args) > 2 else kwargs.get("fields", [])
        else:
            entity_type = kwargs.get("entity_type")
            fields = kwargs.get("fields", [])

        if not fields:
            return entity_type, fields, args, kwargs, set(), {}

        # Handle dotted query fields
        additional_fields, dotted_query_map = self._get_dotted_query_fields(entity_type, fields)

        # Prepare modified args/kwargs
        modified_args = args
        modified_kwargs = kwargs.copy()

        # Add any additional fields needed for dotted queries
        if additional_fields:
            if args:
                new_args = list(args)
                if len(new_args) > 2:
                    new_fields = set(new_args[2])
                    new_fields.update(additional_fields)
                    # Remove the original dotted fields to avoid nesting
                    new_fields = {f for f in new_fields if '.' not in f or f in additional_fields}
                    new_args[2] = list(new_fields)
                modified_args = tuple(new_args)
            else:
                query_fields = set(kwargs.get("fields", []))
                query_fields.update(additional_fields)
                # Remove the original dotted fields to avoid nesting
                query_fields = {f for f in query_fields if '.' not in f or f in additional_fields}
                modified_kwargs["fields"] = list(query_fields)

        return entity_type, fields, modified_args, modified_kwargs, additional_fields, dotted_query_map

    def _process_filters(
        self, filters: List[Dict], parent_entity: Dict
    ) -> List[Union[Dict, List]]:
        """
        Process query filters into FPT API format.

        :param filters: Raw filter conditions
        :param parent_entity: Parent entity reference
        :returns: Processed filters
        """
        processed = []

        for condition in filters:
            if condition.get("active", "true") != "true":
                continue

            if condition.get("conditions"):
                nested = self._process_filters(condition["conditions"], parent_entity)
                if nested:
                    processed.append({
                        "filter_operator": "all"
                        if condition.get("logical_operator") == "and"
                        else "any",
                        "filters": nested,
                    })
            else:
                filter_array = self._create_filter_array(condition, parent_entity)
                if filter_array:
                    processed.append(filter_array)

        return processed

    def _get_dotted_query_fields(self, entity_type: str, fields: List[str]) -> Tuple[
        Set[str], Dict[str, Tuple[str, str, str]]]:
        """
        Identify and parse dotted query fields.

        :param entity_type: The base entity type being queried
        :param fields: List of requested fields
        :returns: A tuple (additional_fields, dotted_query_map), with dotted_query_map a tuple
            (linked_entity_type, link_path, query_field) for each dotted query field.
        """
        additional_fields = set()
        dotted_query_map = {}

        for field in fields:
            parts = field.split('.')
            if len(parts) >= 3:  # We have a dotted field with 3+ parts
                # Last part is the query field name
                query_field = parts[-1]
                # Second to last is the linked entity type
                linked_entity_type = parts[-2]
                # Everything before that is the path to the linked entity
                link_path = '.'.join(parts[:-2])

                # Get schema for the linked entity type
                if linked_entity_type not in self._schema_cache:
                    self._schema_cache[linked_entity_type] = self.schema_field_read(linked_entity_type)
                linked_schema = self._schema_cache[linked_entity_type]

                # Check if this is actually a query field
                if query_field in linked_schema and "query" in linked_schema[query_field].get("properties", {}):
                    # We need to request the link path in the initial query
                    additional_fields.add(link_path)
                    dotted_query_map[field] = (linked_entity_type, link_path, query_field)

        return additional_fields, dotted_query_map

    def _process_dotted_query_fields(
            self,
            entities: List[Entity],
            dotted_query_map: Dict[str, Tuple[str, str, str]]
    ) -> None:
        """
        Process dotted query fields for the given entities.

        :param entities: List of entities to process
        :param dotted_query_map: Mapping of dotted fields to their components
        """
        if not entities or not dotted_query_map:
            return

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = []

            for entity in entities:
                for original_field, (linked_type, link_path, query_field) in dotted_query_map.items():
                    # Get the linked entity from the path
                    linked_entity = entity
                    for path_part in link_path.split('.'):
                        linked_entity = linked_entity.get(path_part, {})
                        if not linked_entity:
                            break

                    if not linked_entity or not isinstance(linked_entity, dict):
                        continue

                    # Get schema for the linked entity type
                    if linked_type not in self._schema_cache:
                        self._schema_cache[linked_type] = self.schema_field_read(linked_type)
                    schema = self._schema_cache[linked_type]

                    # Get query field schema
                    field_schema = schema.get(query_field, {})
                    if not field_schema or "query" not in field_schema.get("properties", {}):
                        continue

                    # Submit the query field resolution
                    future = executor.submit(
                        self._resolve_query_field,
                        field_name=query_field,
                        field_schema=field_schema,
                        parent_entity={"type": linked_type, "id": linked_entity.get("id")}
                    )
                    futures.append((future, entity, original_field))

            # Process results
            for future, entity, field_name in futures:
                try:
                    result = future.result(timeout=30)
                    # Store the result using the original dotted field name
                    entity[field_name] = result
                except Exception as e:
                    logger.error(f"Error processing dotted query field {field_name}: {e}")

    def _create_filter_array(
        self, condition: Dict, parent_entity: Dict
    ) -> Optional[List]:
        """
        Create a filter array for a single condition.

        :param condition: Filter condition
        :param parent_entity: Parent entity reference
        :returns: Filter array or None if invalid
        """
        path = condition.get("path")
        relation = condition.get("relation")
        values = condition.get("values", [])

        if not values:
            return None

        value = values[0]

        if isinstance(value, dict):
            if value.get("valid") == "parent_entity_token":
                return [path, relation, parent_entity]
            elif value.get("id") == 0:
                return None
            else:
                return [path, relation, {"type": value["type"], "id": value["id"]}]
        path_tokens = path.split(".")
        last_field = path_tokens[-1]
        if len(path_tokens) > 1:
            parent_entity_type = path_tokens[-2]
        else:
            parent_entity_type = parent_entity["type"]
        if parent_entity_type not in self._schema_cache:
            self._schema_cache[parent_entity_type] = self.schema_field_read(parent_entity_type)
        parent_entity_schema = self._schema_cache.get(parent_entity_type, {})
        field_schema = parent_entity_schema.get(last_field, {})
        # check if a single value is expected to either pass values[0] or values
        return [path, relation, values]

    def _handle_record_query(
        self, entity_type: str, filters: List, field: str, summary_value: Dict
    ) -> str:
        """
        Handle a record query field.

        :param entity_type: Type of entity to query
        :param filters: Query filters to apply
        :param field: Field to retrieve
        :param summary_value: Query configuration
        :returns: Formatted query result
        """
        order = []
        if summary_value:
            if "column" in summary_value:
                order = [{
                    "field_name": summary_value["column"],
                    "direction": summary_value.get("direction", "asc"),
                }]
            limit = summary_value.get("limit", 1)
        else:
            limit = 1
        results = self.find(
            entity_type=entity_type,
            filters=filters,
            fields=[field],
            order=order,
            limit=limit,
            process_query_fields=False,
        )

        if not results:
            return ""

        formatted_results = []
        for result in results:
            value = result.get(field)
            if isinstance(value, dict):
                the_value = ""
                for key in ["name", "code", "content"]:
                    if key in value:
                        the_value = value[key]
                formatted_results.append(the_value)
            else:
                formatted_results.append(str(value or ""))
        return ", ".join(formatted_results)

    def _handle_aggregate_query(
            self, entity_type: str, filters: List, field: str, aggregate_type: str
    ) -> str:
        """
        Handle an aggregate query field.

        :param entity_type: Type of entity to query
        :param filters: Query filters to apply
        :param field: Field to aggregate
        :param aggregate_type: Type of aggregation
        :returns: Aggregate value as string
        """
        summary = self.summarize(
            entity_type=entity_type,
            filters=filters,
            summary_fields=[{"field": field, "type": aggregate_type}]
        )
        return str(summary["summaries"][field])

    def _handle_percentage_query(
            self, entity_type: str, filters: List, summary_type: str, field: str, summary_value: Union[Dict, str]
    ) -> str:
        """
        Handle a percentage query field.

        :param entity_type: Type of entity to query
        :param filters: Query filters to apply
        :param field: Field to calculate percentage for
        :param summary_type: Type of percentage calculation
        :param summary_value: Value or configuration to compare against
        :returns: Formatted percentage string
        """
        summary = self.summarize(
            entity_type=entity_type,
            filters=filters,
            summary_fields=[{"field": field, "type": summary_type, "value": summary_value}]
        )
        # If summary_value is a string, it's a status value (like 'ip')
        value_str = summary_value if isinstance(summary_value, str) else str(summary_value)
        return f"{summary['summaries'][field]}% {value_str}"

    def _handle_count_query(self, entity_type: str, filters: List) -> str:
        """
        Handle a count query field.

        :param entity_type: Type of entity to query
        :param filters: Query filters to apply
        :returns: Count as string
        """
        summary = self.summarize(
            entity_type=entity_type,
            filters=filters,
            summary_fields=[{"field": "id", "type": "count"}]
        )
        return str(summary["summaries"]["id"])

    def _close_connection(self) -> None:
        """
        Close the current connection pool.
        """
        if self._connection is not None:
            self._connection.clear()
            self._connection = None
