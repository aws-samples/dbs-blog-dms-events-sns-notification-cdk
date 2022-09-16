import json

import pytest

from dms_events_notification import app


@pytest.fixture()
def cw_logs_event():
    """ Generates CloudWatch Logs Event"""

    return {
        {
            'awslogs': {
            'data': 'H4sIAAAAAAAAAE2QW2vCMBiG/0rIjQpWkjSHtnd1Vuc8zNmCTBGpNmiZtqWJG0P87/u6ISxXyfsd3rzPDV+0MelRJ9+VxgEehEm4m0VxHI4i3MXlV6FrkImQPnddyhSVIJ/L46gurxVUsotxbGo+jFOcrEv5XzW2tU4v/8rOWErhDmM1HPblQq3GkzV/4SycvKrJIFn3o7dFxJfTdxg317051Hll87IY5mera4ODDR7M4ml5NE58NZUuMp3h7a9V9KkL23TccJ6Bo6uI8KUnGJGeoq6SlFEu4E4kcQXnEIVzLphHIIzweCMqxhQ42xxg2PQCuaiU1INp4RNCug9IsJ4RxhziOcxPGAuECISPNkkYT3azcA7QlgjOdhUglKT7s0atcDAbz1u9VhLFye7pOYQHakPGhgqiyJ6AVIZoB+UGmUc2hNq1rs75IW0oNK29Q8Dgnx18395/AHGf0VO1AQAA'
            }
        }
    }


def test_lambda_handler(cw_logs_event, mocker):

    ret = app.lambda_handler(cw_logs_event, "")
    data = json.loads(ret["body"])

    assert ret["statusCode"] == 200
    assert "message" in ret["body"]
    assert data["message"] == "SNS Event notification was successful."
    # assert "location" in data.dict_keys()
