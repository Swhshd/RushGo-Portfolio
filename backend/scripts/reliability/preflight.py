"""Read-only, service-scoped activation classification. Never repairs history."""
import argparse
import json
import pathlib
import re
import subprocess
import sys
import unittest


PAYMENT_SQL = """
SELECT json_build_object(
 'rows', (SELECT count(*) FROM payments),
 'legacy_rows', (SELECT count(*) FROM payments WHERE NOT managed),
 'duplicate_logical_groups', (SELECT count(*) FROM
   (SELECT booking_id FROM payments WHERE booking_id IS NOT NULL
    AND booking_id <> '00000000-0000-0000-0000-000000000000'::uuid
    GROUP BY booking_id HAVING count(*) > 1) d),
 'duplicate_provider_intent_groups', (SELECT count(*) FROM
   (SELECT payment_intent_id FROM payments WHERE payment_intent_id IS NOT NULL AND payment_intent_id <> ''
    GROUP BY payment_intent_id HAVING count(*) > 1) d),
 'unknown_intent_rows', (SELECT count(*) FROM payments WHERE NOT managed AND (payment_intent_id IS NULL OR payment_intent_id='')),
 'legacy_provider_verification_rows', (SELECT count(*) FROM payments WHERE NOT managed AND (status='success' OR (payment_intent_id IS NOT NULL AND payment_intent_id<>''))),
 'managed_rows', (SELECT count(*) FROM payments WHERE managed)
);
"""

EVENT_SQL = """
SELECT json_build_object(
 'tiers', (SELECT count(*) FROM ticket_tiers),
 'unknown_baseline_tiers', (SELECT count(*) FROM ticket_tiers WHERE lifecycle_accounting_baseline IS DISTINCT FROM 'verified'),
 'bounds_drift_tiers', (SELECT count(*) FROM ticket_tiers WHERE total_quantity<0 OR available_quantity<0 OR available_quantity>total_quantity),
 'seat_coverage_drift_tiers', (SELECT count(*) FROM ticket_tiers t WHERE t.total_quantity<>(SELECT count(*) FROM seats s WHERE s.ticket_tier_id=t.id AND s.deleted_at IS NULL)),
 'projection_drift_tiers', (SELECT count(*) FROM ticket_tiers t WHERE t.available_quantity<>t.total_quantity-(SELECT COALESCE(SUM((l.debits->>t.id::text)::bigint),0) FROM booking_seat_lifecycles l WHERE l.accounting_state='applied')),
 'unclassified_booked_seats', (SELECT count(*) FROM seats s LEFT JOIN booking_seat_lifecycles l ON l.booking_id=s.booking_id AND l.generation=s.lifecycle_generation WHERE s.status='booked' AND s.deleted_at IS NULL AND (s.lifecycle_generation IS NULL OR l.state IS DISTINCT FROM 'booked'))
);
"""

SAGA_SQL = """
SELECT json_build_object(
 'rows',(SELECT count(*) FROM sagas),
 'legacy_rows',(SELECT count(*) FROM sagas WHERE NOT recovery_managed),
 'duplicate_booking_groups',(SELECT count(*) FROM (SELECT booking_id FROM sagas GROUP BY booking_id HAVING count(*)>1) d),
 'duplicate_step_order_groups',(SELECT count(*) FROM (SELECT saga_id,"order" FROM saga_steps GROUP BY saga_id,"order" HAVING count(*)>1) d),
 'nullable_graph_rows',(SELECT count(*) FROM sagas s WHERE status IS NULL OR current_step_index IS NULL OR EXISTS(SELECT 1 FROM saga_steps st WHERE st.saga_id=s.id AND (st.status IS NULL OR st."order" IS NULL OR st.should_pause_for_payment IS NULL))),
 'unproven_input_rows',(SELECT count(*) FROM sagas WHERE input_snapshot IS NULL OR input_fingerprint IS NULL),
 'incomplete_graph_rows',(SELECT count(*) FROM sagas s WHERE (SELECT count(*) FROM saga_steps st WHERE st.saga_id=s.id)<>5),
 'attention_rows',(SELECT count(*) FROM sagas WHERE recovery_state='attention'),
 'missing_due_rows',(SELECT count(*) FROM sagas WHERE recovery_managed AND recovery_state='ready' AND status NOT IN ('COMPLETED','ROLLED_BACK') AND next_attempt_at IS NULL)
);
"""

NOTIFICATION_SQL = r"""
WITH identities AS (
 SELECT *,CASE WHEN type='welcome_email' THEN payload->>'user_id' ELSE payload->>'booking_id' END AS raw_entity,
  CASE WHEN type='welcome_email' THEN jsonb_typeof(payload->'user_id') ELSE jsonb_typeof(payload->'booking_id') END AS entity_type FROM notifications
), normalized AS (
 SELECT *,CASE WHEN entity_type='string' AND raw_entity ~* '^(urn:uuid:[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\{[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|[0-9a-f]{32})$'
  THEN translate(regexp_replace(lower(raw_entity),'^urn:uuid:','','i'),'-{}','') ELSE NULL END AS entity FROM identities
)
SELECT json_build_object(
 'rows',(SELECT count(*) FROM notifications),
 'legacy_rows',(SELECT count(*) FROM notifications WHERE NOT managed),
 'duplicate_semantic_groups',(SELECT count(*) FROM (
   SELECT type,channel,entity FROM normalized WHERE entity IS NOT NULL AND entity<>'00000000000000000000000000000000'
   GROUP BY type,channel,entity HAVING count(*)>1) d),
 'unproven_identity_rows',(SELECT count(*) FROM normalized WHERE NOT managed AND (
   entity IS NULL OR entity='00000000000000000000000000000000'
   OR type NOT IN ('booking_confirmation','booking_failure','booking_cancellation','welcome_email'))),
 'legacy_unsent_rows',(SELECT count(*) FROM notifications WHERE NOT managed AND status<>'SENT'),
 'attention_rows',(SELECT count(*) FROM notifications WHERE status='ATTENTION' OR last_error IS NOT NULL));
"""

SCHEDULER_SQL = """
SELECT json_build_object(
 'configs',(SELECT count(*) FROM scheduler_configs),
 'invalid_configs',(SELECT count(*) FROM scheduler_configs WHERE timeout IS NULL OR timeout<=0 OR version IS NULL OR version<1 OR version>=2147483647 OR is_enable IS NULL OR interval_expression='' OR name=''),
 'config_expressions',(SELECT COALESCE(json_agg(interval_expression),'[]'::json) FROM scheduler_configs),
 'nullable_outbound',(SELECT count(*) FROM outbound_events WHERE status IS NULL OR payload IS NULL),
 'invalid_outbound',(SELECT count(*) FROM outbound_events WHERE payload IS NOT NULL AND (jsonb_typeof(payload)<>'object' OR jsonb_typeof(payload->'Id') IS DISTINCT FROM 'string' OR jsonb_typeof(payload->'Version') IS DISTINCT FROM 'number'
 OR jsonb_typeof(payload->'Name') IS DISTINCT FROM 'string' OR jsonb_typeof(payload->'IntervalExpression') IS DISTINCT FROM 'string'
 OR jsonb_typeof(payload->'IsEnabled') IS DISTINCT FROM 'boolean' OR jsonb_typeof(payload->'Timeout') IS DISTINCT FROM 'number'
 OR payload->>'Timeout' !~ '^[0-9]+$' OR payload->>'Version' !~ '^[0-9]+$')),
 'missing_config_events',(SELECT count(*) FROM scheduler_configs c WHERE c.version>1 AND NOT EXISTS(
 SELECT 1 FROM outbound_events e WHERE e.topic='scheduler.config.events' AND e.event_type='SchedulerConfigChanged'
 AND e.status IS NOT NULL AND jsonb_typeof(e.payload->'IsEnabled')='boolean' AND jsonb_typeof(e.payload->'Timeout')='number'
 AND jsonb_typeof(e.payload->'Version')='number' AND e.payload->>'Id'=c.id::text AND e.payload->>'Version'=c.version::text
 AND e.payload->>'Name'=c.name AND e.payload->>'IntervalExpression'=c.interval_expression
 AND e.payload->>'IsEnabled'=c.is_enable::text AND e.payload->>'Timeout'=(c.timeout::bigint*1000000000)::text)));
"""

def classify_scheduler(counts):
    counts=dict(counts)
    expressions=counts.pop('config_expressions',None)
    if not isinstance(expressions,list):
        raise ValueError('Scheduler expressions not established')
    # Conservative numeric six-field cron validation. Unsupported spellings
    # remain unclassified rather than being accepted as valid execution facts.
    def valid_expression(expression):
        if not isinstance(expression,str): return False
        fields=expression.split()
        if len(fields)!=6: return False
        for field,(low,high) in zip(fields,((0,59),(0,59),(0,23),(1,31),(1,12),(0,6))):
            for part in field.split(','):
                bits=part.split('/')
                if len(bits)>2 or (len(bits)==2 and (not bits[1].isdigit() or not 1<=int(bits[1])<=high+1)): return False
                if bits[0]=='*': continue
                bounds=bits[0].split('-')
                if len(bounds)>2 or any(not n.isdigit() or not low<=int(n)<=high for n in bounds): return False
                if len(bounds)==2 and int(bounds[0])>int(bounds[1]): return False
        return True
    counts['invalid_configs']+=sum(not valid_expression(e) for e in expressions)
    names=('configs','invalid_configs','nullable_outbound','invalid_outbound','missing_config_events')
    if set(counts)!=set(names) or any(type(counts[n]) is not int or counts[n]<0 for n in names):
        raise ValueError('unclassified Scheduler facts')
    blockers=[name for name in names[1:] if counts[name]]
    return {'status':'BLOCKED' if blockers else 'PASS','counts':dict(counts),
            'unclassified':blockers,'automatic_adoption':False,'mutation':False}

def classify_notification(counts):
    names=('rows','legacy_rows','duplicate_semantic_groups','unproven_identity_rows','legacy_unsent_rows','attention_rows')
    if set(counts)!=set(names) or any(type(counts[n]) is not int or counts[n]<0 for n in names):
        raise ValueError('unclassified Notification facts')
    blockers=[name for name in names[2:] if counts[name]]
    return {'status':'BLOCKED' if blockers else 'PASS','counts':dict(counts),
            'unclassified':blockers,'automatic_adoption':False,'mutation':False}

def classify_saga(counts):
    names=('rows','legacy_rows','duplicate_booking_groups','duplicate_step_order_groups','nullable_graph_rows',
           'unproven_input_rows','incomplete_graph_rows','attention_rows','missing_due_rows')
    if set(counts)!=set(names) or any(type(counts[n]) is not int or counts[n]<0 for n in names):
        raise ValueError('unclassified Saga facts')
    blockers=[name for name in names[1:] if counts[name]]
    return {'status':'BLOCKED' if blockers else 'PASS','counts':dict(counts),
            'unclassified':blockers,'automatic_adoption':False,'mutation':False}


def classify_event(counts):
    names = ('tiers', 'unknown_baseline_tiers', 'bounds_drift_tiers', 'seat_coverage_drift_tiers',
             'projection_drift_tiers', 'unclassified_booked_seats')
    if set(counts) != set(names) or any(type(counts[n]) is not int or counts[n] < 0 for n in names):
        raise ValueError('unclassified Event facts')
    blockers = [name for name in names[1:] if counts[name]]
    return {'status': 'BLOCKED' if blockers else 'PASS', 'counts': dict(counts),
            'unclassified': blockers, 'automatic_adoption': False, 'mutation': False}


def classify_payment(counts):
    """Counts retain ambiguity; duplicate/missing history never authorizes adoption."""
    names = ('rows', 'legacy_rows', 'duplicate_logical_groups', 'duplicate_provider_intent_groups',
             'unknown_intent_rows', 'legacy_provider_verification_rows', 'managed_rows')
    if set(counts) != set(names) or any(type(counts[n]) is not int or counts[n] < 0 for n in names):
        raise ValueError('unclassified Payment facts')
    blockers = [name for name in names[2:6] if counts[name]]
    return {'status': 'BLOCKED' if blockers else 'PASS', 'counts': dict(counts),
            'unclassified': blockers, 'automatic_adoption': False, 'mutation': False}


def run_read_only(manifest):
    if manifest.get('version') != 1:
        raise ValueError('manifest version must be 1')
    project = manifest['compose_project']
    if not re.fullmatch(r'rushgo-reliability-[a-z0-9-]+', project):
        raise ValueError('preserved/non-isolated project refused')
    report = {'mode': 'READ_ONLY', 'project': project, 'services': {}}
    selected = manifest['postgres_containers']
    if not selected or any(name not in ('payment', 'event','saga','notification','scheduler') for name in selected):
        raise ValueError('unsupported scoped activation service')
    for service, sql in [('payment', PAYMENT_SQL), ('event', EVENT_SQL),('saga',SAGA_SQL),('notification',NOTIFICATION_SQL),('scheduler',SCHEDULER_SQL)]:
        if service not in selected:
            continue
        item = manifest['postgres_containers'][service]
        container = item['container']
        database = item['database']
        user = item['user']
        schema = item.get('schema', 'public')
        if not all(re.fullmatch(r'[a-zA-Z0-9_-]+', value) for value in (container, database, user, schema)):
            raise ValueError('invalid scoped PG identity')
        inspection = subprocess.run(['docker', 'inspect', container], capture_output=True, check=True)
        actual = json.loads(inspection.stdout)[0]
        if actual['Config']['Labels'].get('com.docker.compose.project') != project:
            raise ValueError('container is not part of isolated project')
        if not any(m['Type'] == 'volume' and m.get('Name', '').startswith(project + '_') for m in actual['Mounts']):
            raise ValueError('isolated project volume not established')
        prefix = 'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL search_path TO "' + schema + '",public; '
        table,column={'payment':('payments','managed'),'event':('ticket_tiers','lifecycle_accounting_baseline'),'saga':('sagas','recovery_managed'),'notification':('notifications','managed'),'scheduler':('scheduler_configs','version')}[service]
        identity_sql = "SELECT json_build_object('database',current_database(),'managed',EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema='" + schema + "' AND table_name='" + table + "' AND column_name='" + column + "'));"
        identity_result = subprocess.run(['docker', 'exec', container, 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
                                         '-U', user, '-d', database, '-Atc', prefix + identity_sql + ' ROLLBACK;'], capture_output=True)
        if identity_result.returncode:
            raise ValueError('service identity read failed')
        identities = [json.loads(line) for line in identity_result.stdout.decode().splitlines() if line.startswith('{')]
        if len(identities) != 1 or identities[0]['database'] != database:
            raise ValueError('database identity not established')
        if service == 'payment' and not identities[0]['managed']:
            sql = sql.replace('NOT managed', 'TRUE').replace('WHERE managed', 'WHERE FALSE')
        elif service == 'event' and not identities[0]['managed']:
            sql = """SELECT json_build_object(
             'tiers',(SELECT count(*) FROM ticket_tiers),
             'unknown_baseline_tiers',(SELECT count(*) FROM ticket_tiers),
             'bounds_drift_tiers',(SELECT count(*) FROM ticket_tiers WHERE total_quantity<0 OR available_quantity<0 OR available_quantity>total_quantity),
             'seat_coverage_drift_tiers',(SELECT count(*) FROM ticket_tiers t WHERE t.total_quantity<>(SELECT count(*) FROM seats s WHERE s.ticket_tier_id=t.id AND s.deleted_at IS NULL)),
             'projection_drift_tiers',(SELECT count(*) FROM ticket_tiers WHERE available_quantity<>total_quantity),
             'unclassified_booked_seats',(SELECT count(*) FROM seats WHERE status='booked' AND deleted_at IS NULL));"""
        elif service=='saga' and not identities[0]['managed']:
            sql=sql.replace('NOT recovery_managed','TRUE').replace(
                'input_snapshot IS NULL OR input_fingerprint IS NULL','TRUE').replace("recovery_state='attention'","FALSE").replace("recovery_managed AND recovery_state='ready' AND status NOT IN ('COMPLETED','ROLLED_BACK') AND next_attempt_at IS NULL","FALSE")
        elif service=='notification' and not identities[0]['managed']:
            sql=sql.replace('NOT managed','TRUE').replace("status='ATTENTION' OR last_error IS NOT NULL","FALSE")
        query = prefix + sql + ' ROLLBACK;'
        result = subprocess.run(['docker', 'exec', container, 'psql', '-X', '-v', 'ON_ERROR_STOP=1',
                                 '-U', user, '-d', database, '-Atc', query], capture_output=True)
        if result.returncode:
            raise ValueError('service read-only query failed; no activation proof')
        objects = [json.loads(line) for line in result.stdout.decode().splitlines() if line.startswith('{')]
        if len(objects) != 1:
            raise ValueError('service classification result absent')
        report['services'][service] = {'payment':classify_payment,'event':classify_event,'saga':classify_saga,'notification':classify_notification,'scheduler':classify_scheduler}[service](objects[0])
    report['status'] = 'PASS' if all(v['status'] == 'PASS' for v in report['services'].values()) else 'BLOCKED'
    return report


class LegacyFinancialPreflight(unittest.TestCase):
    def test_duplicate_and_unknown_history_blocks_and_preserves_input(self):
        counts = dict(rows=6, legacy_rows=6, duplicate_logical_groups=1,
                      duplicate_provider_intent_groups=1, unknown_intent_rows=2,
                      legacy_provider_verification_rows=1, managed_rows=0)
        before = json.dumps(counts, sort_keys=True)
        result = classify_payment(counts)
        self.assertEqual(result['status'], 'BLOCKED')
        self.assertEqual(result['unclassified'], ['duplicate_logical_groups', 'duplicate_provider_intent_groups',
                                                'unknown_intent_rows', 'legacy_provider_verification_rows'])
        self.assertFalse(result['automatic_adoption'])
        self.assertFalse(result['mutation'])
        self.assertEqual(json.dumps(counts, sort_keys=True), before)

    def test_empty_isolated_database_is_classifiable(self):
        counts = dict(rows=0, legacy_rows=0, duplicate_logical_groups=0,
                      duplicate_provider_intent_groups=0, unknown_intent_rows=0,
                      legacy_provider_verification_rows=0, managed_rows=0)
        self.assertEqual(classify_payment(counts)['status'], 'PASS')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=pathlib.Path)
    parser.add_argument('--read-only', action='store_true')
    parser.add_argument('--self-test', action='store_true')
    args = parser.parse_args()
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(LegacyFinancialPreflight)
        return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    if not args.read_only or args.manifest is None:
        parser.error('--manifest and --read-only are required')
    try:
        report = run_read_only(json.loads(args.manifest.read_text(encoding='utf-8')))
        print(json.dumps(report, indent=2))
        return 0 if report['status'] == 'PASS' else 2
    except (ValueError, KeyError, OSError, subprocess.SubprocessError):
        print(json.dumps({'status': 'BLOCKED', 'reason': 'isolated read-only activation facts not established'}))
        return 2


if __name__ == '__main__':
    sys.exit(main())
