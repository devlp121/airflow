from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from airflow.providers.tableau.hooks.tableau import TableauJobFinishCode
from airflow.providers.tableau.operators.tableau import TableauOperator, TableauHook

import requests

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



    @patch("airflow.providers.tableau.hooks.tableau.Server")
    @patch('requests.get')
    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_missing_workbook_with_mock_response(self, mock_tableau_hook, mock_request, mock_tableau_server):
        """
        Test execute missing workbook
        """
        mock_resp = requests.models.Response()
        mock_resp.status_code = 502
        mock_request.return_value = mock_resp

        mock_tableau_hook.get_all = Mock(return_value=self.mocked_workbooks)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)

        mock_tableau_hook.server.jobs.get_by_id = Mock(
            return_value=Mock(finish_code=TableauJobFinishCode.SUCCESS.value)
        )

        mock_tableau_hook.server.jobs._make_request.return_value = Mock(
            return_value=mock_request
        )
        with TableauHook(tableau_conn_id="tableau_test_password") as tableau_hook:
            tableau_hook.server = mock_tableau_server
            jobs_status = tableau_hook.get_job_status(job_id="j1")
            assert jobs_status

        # Find not added workbook
        operator = TableauOperator(find="test", resource="workbooks", **self.kwargs)

        job_id = operator.execute(context={})

        mock_tableau_hook.wait_for_state.assert_called_once_with(
                    job_id=job_id, check_interval=20, target_state=TableauJobFinishCode.SUCCESS
                )
        
        # with pytest.raises(AirflowException):
        #     operator.execute({})
