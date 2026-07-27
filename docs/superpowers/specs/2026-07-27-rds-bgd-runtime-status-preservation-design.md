# RDS BGD Runtime Status Preservation Design

## Problem

`LOAD MYSQL SERVERS TO RUNTIME` rebuilds the internal
`mysql_aws_rds_bgd_hostgroups` table. Reinserted rows currently receive the
default `NONE` status even when their workers remain active and retain a later
lifecycle phase. An unchanged worker therefore has correct internal state but
publishes an incorrect runtime status until another phase transition occurs.

## Design

Reconcile the runtime table against the incoming configuration, following the
same broad process used for `mysql_servers`:

1. Delete runtime rows whose `writer_hostgroup` is absent from the incoming
   configuration.
2. Update configured columns on existing rows, matching by `writer_hostgroup`
   and deliberately excluding the runtime-only `status` column.
3. Insert newly configured rows with the schema-default `NONE` status.

`reader_hostgroup` is unique. Existing rows whose reader hostgroup changes are
therefore removed before any replacement rows are inserted, avoiding transient
uniqueness conflicts such as two deployments swapping reader hostgroups. Their
current status is carried into the replacement row because the deployment
identity, `writer_hostgroup`, is unchanged.

Runtime-only auto-generated rows that are absent from the incoming
configuration are removed. An auto-generated row that becomes explicitly
configured is updated in place, changes to `auto_generated=0`, and retains its
current status.

Other worker lifecycle behavior remains unchanged.

An in-progress worker must also restore its phase-specific writer placement
after `LOAD MYSQL SERVERS TO RUNTIME` reapplies configured server placement.
During the one-time configuration refresh action, restore the previous writer
only when its identity changed, but always demote the writer identified by the
fresh topology. Other lifecycle phases are unchanged.

## Verification

Keep `test_rds_bgd_repeat_concurrent-t.cpp` unchanged as the regression test.
Its replacement scenario must show that the refreshed cluster and unaffected
clusters retain their public runtime phases after reconciliation, and that an
in-progress refreshed cluster reapplies its writer placement. Then run the
complete AWS RDS BGD simulator TAP group to check for regressions.
