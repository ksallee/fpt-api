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

from concurrent.futures import ThreadPoolExecutor
import logging
import os
import urllib.parse
from typing import Any, Dict, List, Optional, Tuple, Union, Iterator

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
    """Thread-safe FPT client with parallel query field processing."""

    def __init__(
        self,
        *args: Any,
        from_handle: Optional[Shotgun] = None,
        timeout_secs: Optional[float] = None,
        connect: bool = True,
        **kwargs: Any
    ) -> None:
        """Initialize a new FPT client.

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
        """Make an HTTP request to the FPT server.

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
        """Get or create a connection pool manager."""
        if not hasattr(self, "_connection") or self._connection is None:
            self._connection = self._get_urllib3_manager()
        return self._connection

    def _get_urllib3_manager(self) -> urllib3.PoolManager:
        """Create a new connection pool manager."""
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
        """Find entities with parallel query field processing.

        :returns: List of matching entities with resolved fields
        """
        process_query = kwargs.pop("process_query_fields", True)
        entities = super().find(*args, **kwargs)
        if not entities or not process_query:
            return entities
        return self._process_query_fields(entities, args, kwargs)

    def find_one(self, *args: Any, **kwargs: Any) -> Optional[Entity]:
        """Find a single entity with parallel query field processing.

        :returns: Matching entity with resolved fields or None
        """
        process_query = kwargs.pop("process_query_fields", True)
        entity = super().find_one(*args, **kwargs)

        if not entity or not process_query:
            return entity

        processed = self._process_query_fields([entity], args, kwargs)
        return processed[0] if processed else None

    def yield_find(self, *args: Any, **kwargs: Any) -> Iterator[Entity]:
        """Find entities and yield them one by one as they are processed.

        Similar to find() but yields entities as soon as their query fields are processed
        rather than waiting for all entities to be ready.

        :returns: Iterator yielding matching entities with resolved fields
        """
        process_query = kwargs.pop("process_query_fields", True)

        # Get all entities first since the base API doesn't support streaming
        entities = super().find(*args, **kwargs)
        if not entities or not process_query:
            yield from entities
            return

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
            yield from entities
            return

        # Process entities and fields in parallel, yielding as they complete
        max_workers = min(8, len(entities) * len(query_fields))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Process each entity
            for entity in entities:
                entity_id = entity["id"]
                result_entity = entity.copy()
                futures = []

                # Submit futures for each query field
                for field_name, field_schema in query_fields.items():
                    future = executor.submit(
                        self._resolve_query_field,
                        field_name=field_name,
                        field_schema=field_schema,
                        parent_entity={"type": entity["type"], "id": entity_id}
                    )
                    futures.append((future, field_name))

                # Process futures for this entity
                for future, field_name in futures:
                    try:
                        result = future.result(timeout=30)
                        result_entity[field_name] = result
                    except Exception as e:
                        logger.error(f"Error processing query field {field_name}: {e}")
                        result_entity[field_name] = None

                yield result_entity

    def _process_query_fields(
        self, entities: List[Entity], args: Tuple, kwargs: Dict
    ) -> List[Entity]:
        """Process query fields for multiple entities in parallel.

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

    def _resolve_query_field(
        self, field_name: str, field_schema: Dict, parent_entity: Dict
    ) -> str:
        """Resolve a single query field value.

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

    def _process_filters(
        self, filters: List[Dict], parent_entity: Dict
    ) -> List[Union[Dict, List]]:
        """Process query filters into FPT API format.

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

    def _create_filter_array(
        self, condition: Dict, parent_entity: Dict
    ) -> Optional[List]:
        """Create a filter array for a single condition.

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
        """Handle a record query field.

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
        """Handle an aggregate query field.

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
        """Handle a percentage query field.

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
        """Handle a count query field.

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
        """Close the current connection pool."""
        if self._connection is not None:
            self._connection.clear()
            self._connection = None
