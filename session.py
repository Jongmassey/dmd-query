import google.auth
from enum import Enum
from google.api_core import client_info
from google.oauth2 import service_account
from google.cloud.bigquery.dbapi import connect
from sqlframe.bigquery import BigQuerySession


class Table(Enum):
    VMP = "ebmdatalab.dmd.vmp"
    AMP = "ebmdatalab.dmd.amp"
    VTM = "ebmdatalab.dmd.vtm"
    ROUTE = "ebmdatalab.dmd.route"
    ING = "ebmdatalab.dmd.ing"
    VPI = "ebmdatalab.dmd.vpi"
    VMP_ROUTE = "ebmdatalab.dmd.droute"


def session():
    creds = service_account.Credentials.from_service_account_file("bigquery.json")
    client = google.cloud.bigquery.Client(
        project="ebmdatalab",
        credentials=creds,
        location="EU",
        client_info=client_info.ClientInfo(user_agent="sqlframe"),
    )

    conn = connect(client=client)
    return BigQuerySession(conn=conn, default_dataset="ebmdatalab.dmd")


def table(session: BigQuerySession, table: Table):
    return session.table(table.value)
