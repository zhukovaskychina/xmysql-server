# Full Chain Release/Rollback Drill Record Template

## Drill metadata

- Drill ID:
- Date:
- Release owner:
- Rollback owner:
- Data recovery owner:
- Observability owner:
- Business owner:
- Target package:
- Previous package:
- Target configuration:
- Previous configuration:

## Linked evidence

- P0-B recovery report:
- P0-C concurrency report:
- P0-D observability report:
- Default regression gate:

## Timeline

| Event | Timestamp | Owner | Evidence |
|---|---|---|---|
| Drill started |  |  |  |
| Release started |  |  |  |
| First health check completed |  |  |  |
| Rollback decision made |  |  |  |
| Rollback started |  |  |  |
| Package restored |  |  |  |
| Configuration restored |  |  |  |
| Rollback smoke completed |  |  |  |
| Drill finished |  |  |  |

## Release observations

- Error rate:
- P99 latency:
- Active connections:
- Active transactions:
- Lock waits:
- Long transaction alerts:
- Recovery failures:
- Business impact:

## Rollback result

- Rollback triggered: yes/no
- Trigger reason:
- Rollback duration:
- Rollback within accepted window: yes/no
- Previous package active: yes/no
- Previous configuration active: yes/no
- Smoke checks passed: yes/no

## Data recovery result

- Recovery point:
- Replay boundary:
- Backup or snapshot used:
- Consistency checks passed: yes/no
- Notes:

## Final decision

- Drill status: PASS/FAIL
- Approved for next stage: yes/no
- Open follow-up items:
