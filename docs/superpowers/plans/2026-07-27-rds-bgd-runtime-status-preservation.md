# RDS BGD Runtime Status Preservation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve each active AWS RDS BGD deployment's public runtime status when MySQL server configuration is loaded to runtime.

**Architecture:** Reconcile the internal BGD runtime table by `writer_hostgroup`: remove absent rows, update configured fields without touching `status`, and insert new rows. Delete and reinsert only rows whose unique `reader_hostgroup` changes, carrying their existing status into the replacement.

**Tech Stack:** C++17, ProxySQL Hostgroups Manager, SQLite prepared statements, TAP simulator tests.

## Global Constraints

- Keep positive test waits at three seconds or less.
- Do not inspect or document non-public server states.
- Keep `test_rds_bgd_repeat_concurrent-t.cpp` unchanged as the regression test.

---

### Task 1: Reconcile Runtime BGD Rows

**Files:**
- Modify: `lib/MySQL_HostGroups_Manager.cpp`
- Modify: `include/MySQL_HostGroups_Manager.h`
- Test: `test/tap/tests/test_rds_bgd_repeat_concurrent-t.cpp`

**Interfaces:**
- Consumes: `incoming_aws_rds_bgd_hostgroups`, containing configured BGD rows ordered by `writer_hostgroup`.
- Produces: `generate_mysql_aws_rds_bgd_hostgroups_table()`, which reconciles the internal runtime table while preserving `status` for retained writer hostgroups.

- [x] **Step 1: Confirm the regression test fails for status loss**

Run the isolated AWS RDS BGD simulator group on the rebased feature branch.

Expected: `test_rds_bgd_repeat_concurrent-t` fails assertions 35, 37, and 38 because retained workers do not republish status after the runtime table is rebuilt.

- [x] **Step 2: Remove the unconditional BGD table deletion**

In `MySQL_HostGroups_Manager::commit()`, call
`generate_mysql_aws_rds_bgd_hostgroups_table()` without first executing:

```cpp
mydb->execute("DELETE FROM mysql_aws_rds_bgd_hostgroups");
```

- [x] **Step 3: Reconcile retained, changed, new, and removed rows**

Update `generate_mysql_aws_rds_bgd_hostgroups_table()` to:

```text
read existing writer_hostgroup, reader_hostgroup, and status
identify existing rows whose reader_hostgroup changes
delete rows absent from incoming configuration
delete changed-reader rows before replacements to release UNIQUE values
update retained rows' configured columns without assigning status
insert new and changed-reader rows, using NONE only for genuinely new writers
```

Use prepared statements for row updates and inserts. Preserve nullable green
hostgroups with `sqlite3_bind_null`, set config-loaded rows to
`auto_generated=0`, and consume `incoming_aws_rds_bgd_hostgroups` exactly once.

- [x] **Step 4: Update the function documentation**

Describe reconciliation, the `writer_hostgroup` identity, status preservation,
new-row `NONE` behavior, and changed-reader uniqueness handling in the
declaration and definition comments.

- [x] **Step 5: Reapply in-progress writer placement after refresh**

In `MySQL_Monitor::aws_rds_bgd_config_refresh_action()`, always demote the
writer found in the refreshed topology when the worker remains in
`WRITER_SWITCHOVER_IN_PROGRESS`. Restore the old writer only when its identity
changed.

- [x] **Step 6: Build the BGD target**

Run:

```bash
PROXYSQL40=1 make -s -j"$(nproc)" test_rds_bgd
```

Expected: build succeeds without errors.

- [x] **Step 7: Verify the regression passes**

Start fresh isolated infrastructure and run the BGD simulator group.

Expected: `test_rds_bgd_repeat_concurrent-t` passes all assertions, including
35, 37, and 38.

- [x] **Step 8: Verify the complete simulator group**

Run all registered AWS RDS BGD TAP executables against fresh infrastructure.

Expected: all 11 executables pass.

- [x] **Step 9: Commit the implementation**

```bash
git add lib/MySQL_HostGroups_Manager.cpp include/MySQL_HostGroups_Manager.h lib/MySQL_Monitor.cpp
git commit -m "fix: preserve RDS BGD runtime status on reload"
```
