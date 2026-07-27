# RDS BGD Runtime Status Preservation Design

## Problem

`LOAD MYSQL SERVERS TO RUNTIME` rebuilds the internal
`mysql_aws_rds_bgd_hostgroups` table. Reinserted rows currently receive the
default `NONE` status even when their workers remain active and retain a later
lifecycle phase. An unchanged worker therefore has correct internal state but
publishes an incorrect runtime status until another phase transition occurs.

## Design

Before rebuilding the table, snapshot each existing row's
`writer_hostgroup,status` pair. Regenerate configured rows as today, then restore
the saved status for writer hostgroups that still exist.

- Existing deployments retain their last published lifecycle status.
- Newly configured deployments start at the schema default, `NONE`.
- Removed deployments are not restored.
- Worker lifecycle and per-hostgroup refresh behavior remain unchanged.

## Verification

Keep `test_rds_bgd_repeat_concurrent-t.cpp` unchanged as the regression test.
Its replacement scenario must show that the refreshed cluster and unaffected
clusters retain their public runtime phases after the table rebuild. Then run
the complete AWS RDS BGD simulator TAP group to check for regressions.
