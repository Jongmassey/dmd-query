from sqlframe.bigquery import BigQueryDataFrame, BigQuerySession
from sqlframe.bigquery import functions as F
from session import table, Table


def VMPs_by_ing_names(s: BigQuerySession, names: list[str]) -> BigQueryDataFrame:
    ings = table(s, Table.ING).where(F.col("nm").isin(names)).select("id")
    vpis = table(s, Table.VPI)
    vpis = vpis.join(ings, vpis.ing == ings.id).select("vmp")
    vmps = table(s, Table.VMP)
    vmps = vmps.join(vpis, vmps.id == vpis.vmp).select(
        F.col("id"),
        F.col("nm"),
        F.lit("VMP").alias("type"),
    )
    return vmps


def AMPs_from_VMPs(s: BigQuerySession, vmps: BigQueryDataFrame):
    amps = table(s, Table.AMP)
    vmps = vmps.withColumnRenamed("id", "vmp")
    amps = amps.join(vmps, "vmp").select(
        F.col("id"),
        F.col("nm"),
        F.lit("AMP").alias("type"),
    )
    return amps


def VMPs_by_route_names(s: BigQuerySession, names: list[str]) -> BigQueryDataFrame:
    routes = table(s, Table.ROUTE).where(F.col("descr").isin(names)).select("cd")
    vmproutes = table(s, Table.VMP_ROUTE)
    vmproutes = vmproutes.join(routes, routes.cd == vmproutes.route)
    vmps = table(s, Table.VMP)
    vmps = vmps.join(vmproutes, vmps.id == vmproutes.vmp).select(
        F.col("id"),
        F.col("nm"),
        F.lit("VMP").alias("type"),
    )
    return vmps


def opensafely_asthma_oral_prednisolone_medication(
    s: BigQuerySession,
) -> BigQueryDataFrame:
    # https://www.opencodelists.org/codelist/opensafely/asthma-oral-prednisolone-medication/2020-04-27/
    # most used dmd codelist in opensafely (n=39)
    # all oral prednisolone meds
    prednisolone_vmps = VMPs_by_ing_names(s, ["Prednisolone"])
    oral_vmps = VMPs_by_route_names(s, ["Oral"])
    oral_prednisolone_vmps = prednisolone_vmps.intersect(oral_vmps)
    oral_prednisolone_amps = AMPs_from_VMPs(s, oral_prednisolone_vmps)
    return oral_prednisolone_vmps.union(oral_prednisolone_amps)
