import unittest
from unittest.mock import Mock, patch
from requests import Response
from airflow.providers.tableau.sensors.tableau import TableauJobStatusSensor, TableauJobFinishCode, TableauHook

from airflow.exceptions import AirflowSensorTimeout

from airflow import configuration, models
from airflow.utils import db

class TestTableauAirflowProvider(unittest.TestCase):
        
    @patch('requests.get')
    @patch("airflow.providers.tableau.sensors.tableau.TableauHook")
    def test_replicate_502_response(self, mock_tableau_hook, mock_requests):
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

        # Create a mock response with a 502 status code
        mock_response = Mock(spec=Response)
        mock_response.status_code = 502
        
        # Configure the mock requests object to return the mock response
        mock_requests.get.return_value = mock_response

        mock_tableau_hook.get_job_status.return_value = TableauJobFinishCode.ERROR


        # Create an instance of the TableauJobStatusSensor with your configuration
        # Replace the placeholders with your actual configuration
        tableau_job_sensor = TableauJobStatusSensor(
            task_id='tableau_job_sensor',
            tableau_conn_id='tableau_test_password',
            job_id='your_job_id',
            poke_interval=10,  # Adjust the poke interval as needed
            timeout=60,  # Adjust the timeout as needed
        )
        
        # Now, we'll use assertRaises to check if the AirflowSensorTimeout exception
        # is raised when we run the sensor.
        with self.assertRaises(AirflowSensorTimeout):
            tableau_job_sensor.execute(None)
