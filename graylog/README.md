# Graylog dashboard and alerts

Importable Graylog content packs for the audit data this exporter ships.

| file | contents |
|---|---|
| `content-pack-dashboard.json` | four-tab dashboard |
| `content-pack-alerts.json` | three event definitions + a stub mail notification |

Tested on Graylog 7.2.

## Import

System → Content Packs → Upload → select the file → Install. Graylog resolves
the stream by **title**, so the stream your GELF input writes into must be
named `GELF-UDP`, or edit the `stream_title` entity in the JSON before
importing.

The alerts pack ships a **stub** notification — title `Mail`, no sender, no
recipients. Wire it up after import, or repoint the three definitions at a
notification you already have. Nothing will be mailed until you do.

Nothing in either pack is specific to one Atlassian site: every query keys on
`source` (`jira`, `confluence`, `bitbucket-commits`) and the `summary` field.

## Tabs

| tab | what it shows |
|---|---|
| Access and permissions | group membership changes over time, most-changed groups, privileged configuration changes (group/space create + delete, permission and role changes), and a detail table of who was added or removed and to which group |
| Bitbucket | repository activity over time, by repository, by author, and the action-type mix |
| Data movement | Confluence site exports and who ran them, content restrictions added, attribution coverage by category |
| Feed health | messages per day by source, newest audit events — so a stalled exporter is visible rather than silent |

## Read this before trusting the "who" column

**The Jira and Confluence audit log records the actor as the Jira/Confluence
application itself for group and user management events.** So these panels
show *what* changed and *to whom*, but not who made the change. That is a
limitation of the audit log available without Atlassian Guard — not of this
exporter, and not of the queries. It was verified across the full retained
history before the dashboard was built, which is why the "attribution coverage
by category" panel exists: it makes the blind spot measurable instead of
leaving you to assume the field is just occasionally empty.

Bitbucket commit records **do** carry an author, which is why the Bitbucket tab
has a by-author panel and the Jira/Confluence tabs do not.

## Alerts

| alert | fires on | grace | priority |
|---|---|---|---|
| group membership changed | `User added to group`, `User removed from group` | 12h | 2 |
| privileged configuration change | group/space create + delete, permission and role changes, RBAC mode, project deleted, content restriction added | 12h | 3 |
| Confluence site export | `Site export` | 24h | 1 |

All three use `count > 0` — any matching event mails. The rate limiting is the
**grace period**, not a count, and they group by `(source, summary)` so a bulk
IdP sync collapses into one mail per action type rather than one per account.
`backlog_size` is 10 so the mail lists the affected accounts.

There is no site-specific threshold to compute, which is why these ship as-is.
The `Baseline:` line in each description is from one site (~12, ~6 and ~2
events per week); read your own rate off the dashboard's first tab and then
decide whether `> 0` with a long grace suits you, or whether you want a real
threshold.

### The 60-minute window is load-bearing — do not lower it

`timestamp` is the audit record's own `created` time, not ingest time, and this
exporter polls on an interval. So the aggregation window is measured in **event
time** while records arrive late, and a window shorter than the delivery lag
silently matches nothing.

Measured over 30 days on one site, collapsing the exporter's duplicate
re-emissions to the earliest ingest per logical event (94 distinct events):

```
p50 15.9 min   p75 18.4   p90 22.3   p95 28.8   max 497.6

window 10 min -> MISSES 57.4% of events
window 15 min -> MISSES 53.2%
window 20 min -> MISSES 11.7%
window 30 min -> MISSES  1.1%
window 60 min -> MISSES  1.1%
```

30 min is the knee; 60 min costs nothing extra because grace is applied per
event key, so the same event sitting in the window across 12 consecutive
executions still produces exactly one mail. The only event outside 60 min is a
single 8.3h outlier — an exporter catch-up, not a delivery characteristic.

**If your exporter polls less often than every 30 minutes, raise the window.**
