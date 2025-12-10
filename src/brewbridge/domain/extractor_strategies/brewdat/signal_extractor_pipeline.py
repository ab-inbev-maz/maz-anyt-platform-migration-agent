from typing import Any, Dict, List, Optional, Tuple

import yaml

from brewbridge.infrastructure import get_logger

logger = get_logger(__name__)


class BrewdatSignalExtractor:
    """
    Deterministic signal extraction for BrewDat 3.0 artifacts focused on a
    reduced set of normalized_schema_v4 keys. No LLM usage.
    """

    def __init__(
        self,
        default_zone: str = "maz",
        default_landing_zone: str = "maz",
        default_owner: str = "platform",
        default_connector: str = "blob",
        default_source_entity: str = "source_entity_placeholder",
        domain_map: Optional[Dict[str, str]] = None,
    ) -> None:
        self.default_zone = default_zone
        self.default_landing_zone = default_landing_zone
        self.default_owner = default_owner
        self.default_connector = default_connector
        self.default_source_entity = default_source_entity
        self.domain_map = domain_map or {
            "tech": "technology",
            "sale": "sales",
            "sales": "sales",
        }

    def extract(self, raw_artifacts: Dict[str, Any]) -> Dict[str, Any]:
        trigger = raw_artifacts.get("trigger_json", {})
        items = raw_artifacts.get("items", [])

        params = self._trigger_parameters(trigger)
        # Prefer values from the first item config; fallback to trigger params and defaults.
        first_cfg = self._first_cfg(items)
        zone = first_cfg.get("target_zone") or params.get("source_system_zone") or self.default_zone
        landing_zone = (
            first_cfg.get("target_zone") or params.get("target_zone") or self.default_landing_zone
        )
        domain_raw = (
            first_cfg.get("target_business_domain")
            or params.get("target_business_domain")
            or "unknown_domain"
        )
        domain = self._map_domain(domain_raw)
        pipeline_source_system = (
            first_cfg.get("source_system")
            or first_cfg.get("source_system_raw")
            or params.get("source_system")
            or params.get("source_system_raw")
            or "unknown_source_system"
        )
        pipeline_name = trigger.get("name") or "unknown_pipeline"
        cron_val, schedule_raw = (
            self._derive_cron(trigger) if self._is_schedule_trigger(trigger) else (None, None)
        )
        schedule = cron_val if cron_val else schedule_raw

        tables, counts = self._build_tables(
            items=items,
            zone=zone,
            domain=domain,
            raw_artifacts=raw_artifacts,
            schedule=schedule,
        )

        return {
            "pipeline": {
                "zone": zone,
                "landing_zone": landing_zone,
                "domain": domain,
                "pipeline": pipeline_name,
                "schedule": schedule,
                "owner": self.default_owner,
                "connector": self.default_connector,
                "source_system": pipeline_source_system,
                "source_entity": "remove_later",
                "counts": counts,
            },
            "tables": tables,
        }

    # ------------------------------------------------------------------ #
    # Table construction
    # ------------------------------------------------------------------ #
    def _build_tables(
        self,
        items: List[Dict[str, Any]],
        zone: str,
        domain: str,
        raw_artifacts: Dict[str, Any],
        schedule: str,
    ) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, int]]:
        tables: Dict[str, Dict[str, Any]] = {}
        counts = {"bronze": 0, "silver": 0}

        for item in items:
            bronze_cfg = item.get("bronze_config") or {}
            silver_cfg = item.get("silver_config") or {}

            # Choose the primary config for table construction: prefer bronze, fallback to silver if bronze is absent.
            primary_cfg = bronze_cfg or silver_cfg or {}
            if not primary_cfg:
                continue

            target_layer = "brz" if bronze_cfg else "slv"
            table_data = self._build_table_entry(
                cfg=primary_cfg,
                target_layer=target_layer,
                zone=zone,
                domain=domain,
                source_system_fallback=None,
                country_fallback=None,
                raw_artifacts=raw_artifacts,
                schedule=schedule,
            )

            # If silver config exists, mark transformations as "y" and count silver.
            if silver_cfg:
                table_data["transformations"] = "y"
                counts["silver"] += 1

            key = table_data["target_entity"]
            tables[key] = table_data
            if bronze_cfg:
                counts["bronze"] += 1

        return tables, counts

    def _build_table_entry(
        self,
        cfg: Dict[str, Any],
        target_layer: str,
        zone: str,
        domain: str,
        source_system_fallback: Optional[str],
        country_fallback: Optional[str],
        raw_artifacts: Dict[str, Any],
        schedule: str,
    ) -> Dict[str, Any]:
        zone_local = cfg.get("target_zone") or zone
        domain_local = self._map_domain(cfg.get("target_business_domain") or domain)
        source_system = (
            cfg.get("source_system")
            or cfg.get("source_system_raw")
            or source_system_fallback
            or "unknown_source_system"
        )
        source_system_country = (
            cfg.get("source_system_country")
            or country_fallback
            or cfg.get("source_raw_zone")
            or "unknown_country"
        )
        target_entity = cfg.get("target_table") or "unknown_target_table"
        source_entity = cfg.get("source_table") or self.default_source_entity

        if target_layer == "brz":
            table_name = f"brz_{zone_local}_{domain_local}_{source_system}_{source_system_country}"
        else:
            table_name = cfg.get("target_table") or cfg.get("source_table") or target_entity

        transformations = "y" if target_layer == "slv" else "n"
        acl = "y" if self._has_access_groups(raw_artifacts) else "n"
        connection_id = self._extract_connection_id(raw_artifacts)

        return {
            "target_layer": target_layer,
            "domain": domain_local,
            "table_name": table_name,
            "owner": self.default_owner,
            "connector": self.default_connector,
            "target_entity": target_entity,
            "connection_id": connection_id,
            "transformations": transformations,
            "acl": acl,
        }

    # ------------------------------------------------------------------ #
    # Helper extractors
    # ------------------------------------------------------------------ #
    def _trigger_parameters(self, trigger: Dict[str, Any]) -> Dict[str, Any]:
        return trigger.get("properties", {}).get("pipelines", [{}])[0].get("parameters", {})

    def _derive_cron(
        self, trigger_json: Dict[str, Any]
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        """
        Best-effort cron derivation using ADF schedule trigger.
        Returns a tuple: (cron_string | None, raw_schedule_signal | None).
        If cron cannot be inferred, raw_schedule_signal will contain the recurrence block
        so the LLM can handle complex patterns.
        """
        type_props = (
            trigger_json.get("properties", {}).get("typeProperties", {})
            if isinstance(trigger_json.get("properties"), dict)
            else trigger_json.get("typeProperties", {})
        )
        recurrence = type_props.get("recurrence", {}) if isinstance(type_props, dict) else {}
        schedule = recurrence.get("schedule", {}) if isinstance(recurrence, dict) else {}

        minutes = self._first_int(schedule.get("minutes"))
        hours = self._first_int(schedule.get("hours"))

        if minutes is None or hours is None:
            start_time = recurrence.get("startTime")
            if isinstance(start_time, str) and "T" in start_time:
                try:
                    time_part = start_time.split("T", 1)[1]
                    h, m, *_ = time_part.split(":")
                    hours = int(h) if hours is None else hours
                    minutes = int(m) if minutes is None else minutes
                except Exception:
                    pass

        if minutes is None and hours is None:
            return None, recurrence if recurrence else type_props

        minute_str = str(minutes) if minutes is not None else "*"
        hour_str = str(hours) if hours is not None else "*"
        return f"{minute_str} {hour_str} * * *", recurrence if recurrence else type_props

    def _first_int(self, value: Any) -> Optional[int]:
        if isinstance(value, list) and value:
            try:
                return int(value[0])
            except (TypeError, ValueError):
                return None
        if isinstance(value, int):
            return value
        return None

    def _is_schedule_trigger(self, trigger_json: Dict[str, Any]) -> bool:
        prop_type = trigger_json.get("properties", {}).get("type")
        top_type = trigger_json.get("type")
        return prop_type == "ScheduleTrigger" or top_type == "ScheduleTrigger"

    def _first_cfg(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not items:
            return {}
        first = items[0]
        return (first.get("silver_config") or first.get("bronze_config")) or {}

    def _map_domain(self, domain: Optional[str]) -> str:
        if not domain or not isinstance(domain, str):
            return "unknown_domain"
        cleaned = domain.strip()
        key = cleaned.lower()
        if key in self.domain_map:
            return self.domain_map[key]
        return cleaned

    def _extract_connection_id(self, raw_artifacts: Dict[str, Any]) -> str:
        """
        Extract connection_id from manifest.yaml if present in raw_artifacts.
        Fallback to empty string if not found.
        """
        manifest_content = raw_artifacts.get("manifest_yaml")
        if not manifest_content:
            return "sap-test-secret"

        try:
            manifest = (
                yaml.safe_load(manifest_content)
                if isinstance(manifest_content, str)
                else manifest_content
            )
            if isinstance(manifest, dict):
                conn = manifest.get("connection_id")
                if conn:
                    return str(conn).strip()
        except Exception:
            logger.debug("Could not parse manifest_yaml for connection_id.")
        return "sap-test-secret"

    def _has_access_groups(self, raw_artifacts: Dict[str, Any]) -> bool:
        metadata_content = raw_artifacts.get("metadata_yaml")
        if not metadata_content:
            return False
        try:
            metadata = (
                yaml.safe_load(metadata_content)
                if isinstance(metadata_content, str)
                else metadata_content
            )
            if isinstance(metadata, dict):
                access_groups = metadata.get("state", {}).get("metadata", {}).get("access_groups")
                return access_groups is not None
        except Exception:
            logger.debug("Could not parse metadata_yaml for access_groups.")
        return False
