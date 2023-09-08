from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from airflow.providers.tableau.hooks.tableau import TableauJobFinishCode
from airflow.providers.tableau.operators.tableau import TableauOperator, TableauHook
from tableauserverclient.server.endpoint.endpoint import Endpoint
from tableauserverclient.models import JobItem
from tableauserverclient.server import Server

import requests
from datetime import datetime

from airflow import configuration, models
from airflow.utils import db
from airflow.exceptions import AirflowException


class TestMissingWorkbook:
    """
    Tableau test class to replicate 5xx response
    """

    def setup_method(self):

        configuration.conf.load_test_config()

        db.merge_conn(
            models.Connection(
                conn_id="tableau_test_password",
                conn_type="tableau",
                host="tableau",
                login="user",
                password="password",
                extra='{"site_id": "my_site"}',
            )
        )
        self.mocked_workbooks = []
        self.mock_datasources = []

        for i in range(3):
            mock_workbook = Mock()
            mock_workbook.id = i
            mock_workbook.name = f"wb_{i}"
            self.mocked_workbooks.append(mock_workbook)

            mock_datasource = Mock()
            mock_datasource.id = i
            mock_datasource.name = f"ds_{i}"
            self.mock_datasources.append(mock_datasource)

        self.kwargs = {
            "site_id": "test_site",
            "task_id": "task",
            "dag": None,
            "match_with": "name",
            "method": "refresh",
        }


    @patch('requests.get')
    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_already_queued_workbook_with_mock_response(self, mock_tableau_hook, mock_request):
        """
        Test execute already queued workbook
        """
    
        mock_resp = requests.models.Response()
        mock_resp.status_code = 502
        mock_resp.reason = "Not Found"
        mock_resp._content = b"Already queued this workbook"
        mock_request.return_value = mock_resp

        mock_tableau_hook.get_all = Mock(return_value=self.mocked_workbooks)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        job_item = JobItem(id_="2",job_type="no_idea",progress="completed",created_at=datetime(2023,9,2))

        def mock_server_refresh(workbook_id):
            # new_job = JobItem.from_response(mock_resp.content, mock_tableau_hook.server.namespace)[0]
            return job_item

        mock_tableau_hook.server.workbooks.refresh = Mock(side_effect=mock_server_refresh)
        
        operator = TableauOperator(blocking_refresh=False, find="wb_2", resource="workbooks", **self.kwargs)

        operator.execute(context={})

        mock_tableau_hook.server.workbooks.refresh.assert_called_once_with(2)
     
