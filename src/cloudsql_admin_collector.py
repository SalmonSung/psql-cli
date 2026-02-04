from __future__ import annotations

from typing import Any, Dict

import google.auth
from google.auth.transport.requests import AuthorizedSession


class CloudSQLAdminCollector:
    def __init__(
        self,
        project_id: str,
        instance_id: str,
    ) -> None:
        if not isinstance(project_id, str) or not project_id.strip():
            raise ValueError("project_id must be a non-empty string")
        if not isinstance(instance_id, str) or not instance_id.strip():
            raise ValueError("instance_id must be a non-empty string")

        self.project_id = project_id
        self.instance_id = instance_id

        creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self._session = AuthorizedSession(creds)

    def _instance_url(self) -> str:
        return (
            "https://sqladmin.googleapis.com/sql/v1beta4/projects/"
            f"{self.project_id}/instances/{self.instance_id}"
        )

    def get_instance_summary(self) -> Dict[str, Any]:
        """
        Fetch Cloud SQL instance details and return a summary dictionary.

        Returns:
            {
                "tier": "db-custom-4-15360",
                "cpu_core": 4,
                "memory_mb": 15360,
                "disk": "200 GB PD_SSD",
                "disk_size_gb": 200,
                "availability": "REGIONAL",
                "region": "europe-west1",
            }
        """
        response = self._session.get(self._instance_url(), timeout=30)
        response.raise_for_status()
        payload: Dict[str, Any] = response.json()

        settings = payload.get("settings", {})
        disk_size_gb = settings.get("dataDiskSizeGb")
        disk_type = settings.get("dataDiskType")

        disk = None
        if disk_size_gb is not None or disk_type is not None:
            disk_parts = []
            if disk_size_gb is not None:
                disk_parts.append(f"{disk_size_gb} GB")
            if disk_type:
                disk_parts.append(str(disk_type))
            disk = " ".join(disk_parts)
        tier_split = settings.get("tier").split("-")


        return {
            "tier": settings.get("tier"),
            "cpu_core":int(tier_split[-2]),
            "memory_mb": int(tier_split[-1]),
            "disk": disk,
            "disk_size_gb": int(disk_size_gb),
            "availability": settings.get("availabilityType"),
            "region": payload.get("region"),
        }