import datetime
import json
from typing import Dict, List, Optional
from unittest.mock import patch

import celery
import requests.exceptions
from boto3 import resource
from botocore.client import Config
from django.http import HttpResponse
from freezegun import freeze_time
from rest_framework import status

from posthog.api.test.test_organization import create_organization
from posthog.api.test.test_team import create_team
from posthog.api.test.test_user import create_user
from posthog.models import DashboardTemplate
from posthog.models.dashboard import Dashboard
from posthog.models.exported_asset import ExportedAsset
from posthog.models.filters.filter import Filter
from posthog.models.insight import Insight
from posthog.models.team import Team
from posthog.settings import (
    OBJECT_STORAGE_ACCESS_KEY_ID,
    OBJECT_STORAGE_BUCKET,
    OBJECT_STORAGE_ENDPOINT,
    OBJECT_STORAGE_SECRET_ACCESS_KEY,
)
from posthog.tasks import exporter
from posthog.test.base import APIBaseTest, _create_event, flush_persons_and_events

TEST_ROOT_BUCKET = "test_exports"


class TestExports(APIBaseTest):
    exported_asset: ExportedAsset = None  # type: ignore
    dashboard: Dashboard = None  # type: ignore
    insight: Insight = None  # type: ignore
    dashboard_template: DashboardTemplate = None  # type: ignore
    global_dashboard_template: DashboardTemplate = None  # type: ignore

    def teardown_method(self, method) -> None:
        s3 = resource(
            "s3",
            endpoint_url=OBJECT_STORAGE_ENDPOINT,
            aws_access_key_id=OBJECT_STORAGE_ACCESS_KEY_ID,
            aws_secret_access_key=OBJECT_STORAGE_SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1",
        )
        bucket = s3.Bucket(OBJECT_STORAGE_BUCKET)
        bucket.objects.filter(Prefix=TEST_ROOT_BUCKET).delete()

    insight_filter_dict = {"events": [{"id": "$pageview"}], "properties": [{"key": "$browser", "value": "Mac OS X"}]}

    @classmethod
    def setUpTestData(cls):
        super().setUpTestData()

        cls.dashboard = Dashboard.objects.create(team=cls.team, name="example dashboard", created_by=cls.user)
        cls.insight = Insight.objects.create(
            filters=Filter(data=cls.insight_filter_dict).to_dict(),
            team=cls.team,
            created_by=cls.user,
            name="example insight",
        )
        cls.exported_asset = ExportedAsset.objects.create(
            team=cls.team, dashboard_id=cls.dashboard.id, export_format="image/png"
        )
        cls.dashboard_template = DashboardTemplate.objects.create(team=cls.team, template_name="example template")

        cls.global_dashboard_template = DashboardTemplate.objects.create(
            team=cls.team, template_name="global template", scope="global"
        )

    @patch("posthog.api.exports.exporter")
    def test_can_create_new_valid_export_dashboard(self, mock_exporter_task) -> None:
        response = self.client.post(
            f"/api/projects/{self.team.id}/exports", {"export_format": "image/png", "dashboard": self.dashboard.id}
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        data = response.json()
        self.assertEqual(
            data,
            {
                "id": data["id"],
                "created_at": data["created_at"],
                "dashboard": self.dashboard.id,
                "export_format": "image/png",
                "filename": "export-example-dashboard.png",
                "has_content": False,
                "insight": None,
                "export_context": None,
            },
        )

        mock_exporter_task.export_asset.delay.assert_called_once_with(data["id"])

    @patch("posthog.api.exports.exporter")
    def test_swallow_missing_schema_and_allow_front_end_to_poll(self, mock_exporter_task) -> None:
        # regression test see https://github.com/PostHog/posthog/issues/11204

        mock_exporter_task.get.side_effect = requests.exceptions.MissingSchema("why is this raised?")

        response = self.client.post(
            f"/api/projects/{self.team.id}/exports",
            {
                "export_format": "text/csv",
                "export_context": {
                    "path": f"api/projects/{self.team.id}/insights/trend/?insight=TRENDS&events=%5B%7B%22id%22%3A%22search%20filtered%22%2C%22name%22%3A%22search%20filtered%22%2C%22type%22%3A%22events%22%2C%22order%22%3A0%7D%5D&actions=%5B%5D&display=ActionsTable&interval=day&breakdown=filters&new_entity=%5B%5D&properties=%5B%5D&breakdown_type=event&filter_test_accounts=false&date_from=-14d"
                },
            },
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, msg=f"was not HTTP 201 😱 - {response.json()}")
        data = response.json()
        mock_exporter_task.export_asset.delay.assert_called_once_with(data["id"])

    @patch("posthog.api.exports.exporter")
    @freeze_time("2021-08-25T22:09:14.252Z")
    def test_can_create_new_valid_export_insight(self, mock_exporter_task) -> None:
        response = self.client.post(
            f"/api/projects/{self.team.id}/exports", {"export_format": "application/pdf", "insight": self.insight.id}
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        data = response.json()
        self.assertEqual(
            data,
            {
                "id": data["id"],
                "created_at": data["created_at"],
                "insight": self.insight.id,
                "export_format": "application/pdf",
                "filename": "export-example-insight.pdf",
                "has_content": False,
                "dashboard": None,
                "export_context": None,
            },
        )

        self._assert_logs_the_activity(
            insight_id=self.insight.id,
            expected=[
                {
                    "user": {"first_name": self.user.first_name, "email": self.user.email},
                    "activity": "exported",
                    "created_at": "2021-08-25T22:09:14.252000Z",
                    "scope": "Insight",
                    "item_id": str(self.insight.id),
                    "detail": {
                        "changes": [
                            {
                                "action": "exported",
                                "after": "application/pdf",
                                "before": None,
                                "field": "export_format",
                                "type": "Insight",
                            }
                        ],
                        "trigger": None,
                        "name": self.insight.name,
                        "short_id": self.insight.short_id,
                    },
                }
            ],
        )

        mock_exporter_task.export_asset.delay.assert_called_once_with(data["id"])

    def test_errors_if_missing_related_instance(self) -> None:
        response = self.client.post(f"/api/projects/{self.team.id}/exports", {"export_format": "image/png"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        assert response.json() == {
            "attr": None,
            "code": "invalid_input",
            "detail": "Either dashboard, insight, or export_context is required for an export.",
            "type": "validation_error",
        }

    def test_errors_if_bad_format(self) -> None:
        response = self.client.post(f"/api/projects/{self.team.id}/exports", {"export_format": "not/allowed"})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.json(),
            {
                "attr": "export_format",
                "code": "invalid_choice",
                "detail": '"not/allowed" is not a valid choice.',
                "type": "validation_error",
            },
        )

    @patch("posthog.api.exports.exporter")
    def test_will_respond_even_if_task_timesout(self, mock_exporter_task) -> None:
        mock_exporter_task.export_asset.delay.return_value.get.side_effect = celery.exceptions.TimeoutError("timed out")
        response = self.client.post(
            f"/api/projects/{self.team.id}/exports", {"export_format": "application/pdf", "insight": self.insight.id}
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    @patch("posthog.api.exports.exporter")
    def test_will_error_if_export_unsupported(self, mock_exporter_task) -> None:
        mock_exporter_task.export_asset.delay.return_value.get.side_effect = NotImplementedError("not implemented")
        response = self.client.post(
            f"/api/projects/{self.team.id}/exports", {"export_format": "application/pdf", "insight": self.insight.id}
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.json(),
            {
                "attr": "export_format",
                "code": "invalid_input",
                "detail": "This type of export is not supported for this resource.",
                "type": "validation_error",
            },
        )

    def test_will_error_if_dashboard_missing(self) -> None:
        response = self.client.post(
            f"/api/projects/{self.team.id}/exports", {"export_format": "application/pdf", "dashboard": 54321}
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.json(),
            {
                "attr": "dashboard",
                "code": "does_not_exist",
                "detail": 'Invalid pk "54321" - object does not exist.',
                "type": "validation_error",
            },
        )

    def test_will_error_if_export_contains_other_team_dashboard(self) -> None:
        other_team = Team.objects.create(
            organization=self.organization,
            api_token=self.CONFIG_API_TOKEN + "2",
            test_account_filters=[
                {"key": "email", "value": "@posthog.com", "operator": "not_icontains", "type": "person"}
            ],
        )
        other_dashboard = Dashboard.objects.create(
            team=other_team, name="example dashboard other", created_by=self.user
        )

        response = self.client.post(
            f"/api/projects/{self.team.id}/exports",
            {"export_format": "application/pdf", "dashboard": other_dashboard.id},
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.json(),
            {
                "attr": "dashboard",
                "code": "invalid_input",
                "detail": "This dashboard does not belong to your team.",
                "type": "validation_error",
            },
        )

    def test_will_error_if_export_contains_other_team_insight(self) -> None:
        other_team = Team.objects.create(
            organization=self.organization,
            api_token=self.CONFIG_API_TOKEN + "2",
            test_account_filters=[
                {"key": "email", "value": "@posthog.com", "operator": "not_icontains", "type": "person"}
            ],
        )
        other_insight = Insight.objects.create(
            filters=Filter(data=self.insight_filter_dict).to_dict(), team=other_team, created_by=self.user
        )

        response = self.client.post(
            f"/api/projects/{self.team.id}/exports", {"export_format": "application/pdf", "insight": other_insight.id}
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            response.json(),
            {
                "attr": "insight",
                "code": "invalid_input",
                "detail": "This insight does not belong to your team.",
                "type": "validation_error",
            },
        )

    @patch("posthog.tasks.exports.csv_exporter.requests.request")
    def test_can_download_a_csv(self, patched_request) -> None:
        with self.settings(SITE_URL="http://testserver"):

            _create_event(event="event_name", team=self.team, distinct_id="2", properties={"$browser": "Chrome"})
            expected_event_id = _create_event(
                event="event_name", team=self.team, distinct_id="2", properties={"$browser": "Safari"}
            )
            second_expected_event_id = _create_event(
                event="event_name", team=self.team, distinct_id="2", properties={"$browser": "Safari"}
            )
            third_expected_event_id = _create_event(
                event="event_name", team=self.team, distinct_id="2", properties={"$browser": "Safari"}
            )
            flush_persons_and_events()

            after = (datetime.datetime.now() - datetime.timedelta(minutes=10)).isoformat()

            def requests_side_effect(*args, **kwargs):
                return self.client.get(kwargs["url"], kwargs["json"], **kwargs["headers"])

            patched_request.side_effect = requests_side_effect

            response = self.client.post(
                f"/api/projects/{self.team.id}/exports",
                {
                    "export_format": "text/csv",
                    "export_context": {
                        "path": "&".join(
                            [
                                f"/api/projects/{self.team.id}/events?orderBy=%5B%22-timestamp%22%5D",
                                "properties=%5B%7B%22key%22%3A%22%24browser%22%2C%22value%22%3A%5B%22Safari%22%5D%2C%22operator%22%3A%22exact%22%2C%22type%22%3A%22event%22%7D%5D",
                                f"after={after}",
                            ]
                        )
                    },
                },
            )
            self.assertEqual(
                response.status_code, status.HTTP_201_CREATED, msg=f"was not HTTP 201 😱 - {response.json()}"
            )
            instance = response.json()

            # limit the query to force it to page against the API
            with self.settings(OBJECT_STORAGE_ENABLED=False):
                exporter.export_asset(instance["id"], limit=1)

            download_response: Optional[HttpResponse] = None
            attempt_count = 0
            while attempt_count < 10 and not download_response:
                download_response = self.client.get(
                    f"/api/projects/{self.team.id}/exports/{instance['id']}/content?download=true"
                )
                attempt_count += 1

            if not download_response:
                self.fail("must have a response by this point")  # hi mypy

            self.assertEqual(download_response.status_code, status.HTTP_200_OK)
            self.assertIsNotNone(download_response.content)
            file_content = download_response.content.decode("utf-8")
            file_lines = file_content.split("\n")
            # has a header row and at least two other rows
            # don't care if the DB hasn't been reset before the test
            self.assertTrue(len(file_lines) > 3)
            self.assertIn(expected_event_id, file_content)
            self.assertIn(second_expected_event_id, file_content)
            self.assertIn(third_expected_event_id, file_content)
            for line in file_lines[1:]:  # every result has to match the filter though
                if line != "":  # skip the final empty line of the file
                    self.assertIn("Safari", line)

    @patch("posthog.tasks.exports.json_exporter.requests.request")
    def test_can_create_new_valid_export_dashboard_template(self, patched_request) -> None:
        def requests_side_effect(*args, **kwargs):
            return self.client.get(kwargs["url"], kwargs["json"], **kwargs["headers"])

        patched_request.side_effect = requests_side_effect

        response = self.client.post(
            f"/api/projects/{self.team.id}/exports",
            {
                "export_format": "application/json",
                "export_context": {
                    "filename": "my-incredible-dashboard.json",
                    "path": "&".join(
                        [
                            f"/api/projects/{self.team.id}/dashboard_templates/{self.dashboard_template.id}",
                        ]
                    ),
                },
            },
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        exported_assert = response.json()

        download_response: Optional[HttpResponse] = None
        attempt_count = 0
        while attempt_count < 10 and not download_response:
            download_response = self.client.get(
                f"/api/projects/{self.team.id}/exports/{exported_assert['id']}/content?download=true"
            )
            attempt_count += 1

        if not download_response:
            self.fail("must have a response by this point")  # hi mypy

        self.assertEqual(download_response.status_code, status.HTTP_200_OK)
        self.assertIsNotNone(download_response.content)
        file_content = download_response.content.decode("utf-8")
        exported_asset_as_dict = json.loads(file_content)
        assert exported_asset_as_dict == {
            "dashboard_description": None,
            "dashboard_filters": None,
            "id": str(self.dashboard_template.id),
            "scope": "project",
            "source_dashboard": None,
            "tags": [],
            "template_name": "example template",
            "tiles": [],
        }

    @patch("posthog.tasks.exports.json_exporter.requests.request")
    def test_cannot_create_new_export_dashboard_template_for_a_different_team(self, patched_request) -> None:
        def requests_side_effect(*args, **kwargs):
            return self.client.get(kwargs["url"], kwargs["json"], **kwargs["headers"])

        patched_request.side_effect = requests_side_effect

        another_organization = create_organization(name="org two")
        org_two_team_one = create_team(organization=another_organization)
        org_two_user = create_user(
            email="team_two_user@posthog.com", password="1234", organization=another_organization
        )
        self.client.force_login(org_two_user)
        response = self.client.post(
            f"/api/projects/{org_two_team_one.id}/exports",
            {
                "export_format": "application/json",
                "export_context": {
                    "filename": "my-incredible-dashboard.json",
                    "path": "&".join(
                        [
                            f"/api/projects/{org_two_team_one.id}/dashboard_templates/{self.dashboard_template.id}",
                        ]
                    ),
                },
            },
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    @patch("posthog.tasks.exports.json_exporter.requests.request")
    def test_can_create_new_export_dashboard_template_for_global_template_from_different_team(
        self, patched_request
    ) -> None:
        def requests_side_effect(*args, **kwargs):
            return self.client.get(kwargs["url"], kwargs["json"], **kwargs["headers"])

        patched_request.side_effect = requests_side_effect

        another_organization = create_organization(name="org two")
        org_two_team_one = create_team(organization=another_organization)
        org_two_user = create_user(
            email="team_two_user@posthog.com", password="1234", organization=another_organization
        )
        self.client.force_login(org_two_user)
        response = self.client.post(
            f"/api/projects/{org_two_team_one.id}/exports",
            {
                "export_format": "application/json",
                "export_context": {
                    "filename": "my-incredible-dashboard.json",
                    "path": "&".join(
                        [
                            f"/api/projects/{org_two_team_one.id}/dashboard_templates/{self.global_dashboard_template.id}",
                        ]
                    ),
                },
            },
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def _get_insight_activity(self, insight_id: int, expected_status: int = status.HTTP_200_OK):
        url = f"/api/projects/{self.team.id}/insights/{insight_id}/activity"
        activity = self.client.get(url)
        self.assertEqual(activity.status_code, expected_status)
        return activity.json()

    def _assert_logs_the_activity(self, insight_id: int, expected: List[Dict]) -> None:
        activity_response = self._get_insight_activity(insight_id)

        activity: List[Dict] = activity_response["results"]

        self.maxDiff = None
        self.assertEqual(activity, expected)


class TestExportMixin(APIBaseTest):
    def _get_export_output(self, path: str) -> List[str]:
        """
        Use this function to test the CSV output of exports in other tests
        """
        with self.settings(SITE_URL="http://testserver", OBJECT_STORAGE_ENABLED=False):
            with patch("posthog.tasks.exports.csv_exporter.requests.request") as patched_request:

                def requests_side_effect(*args, **kwargs):
                    return self.client.get(kwargs["url"], kwargs["json"], **kwargs["headers"])

                patched_request.side_effect = requests_side_effect

                response = self.client.post(
                    f"/api/projects/{self.team.pk}/exports/",
                    {
                        "export_context": {
                            "max_limit": 10000,
                            "path": path,
                        },
                        "export_format": "text/csv",
                    },
                )
                download_response = self.client.get(
                    f"/api/projects/{self.team.id}/exports/{response.json()['id']}/content?download=true"
                )
                return [str(x) for x in download_response.content.splitlines()]
