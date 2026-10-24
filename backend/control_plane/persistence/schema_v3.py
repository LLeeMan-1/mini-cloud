"""Frozen version 3 DDL used only by historical migrations and upgrade tests."""

SCHEMA = [('table',
  'job_attempts',
  'job_attempts',
  'CREATE TABLE job_attempts (\n'
  '\tid INTEGER NOT NULL, \n'
  '\tjob_id INTEGER NOT NULL, \n'
  '\tnumber INTEGER NOT NULL, \n'
  '\tworker_id INTEGER NOT NULL, \n'
  '\tstate VARCHAR(20) NOT NULL, \n'
  '\tclaim_id VARCHAR(36), \n'
  '\tassigned_at DATETIME NOT NULL, \n'
  '\tclaimed_at DATETIME, \n'
  '\tfinished_at DATETIME, \n'
  '\tfailure_code VARCHAR(50), \n'
  '\tretryable BOOLEAN NOT NULL, \n'
  '\tresult JSON, \n'
  '\terror VARCHAR(2000), \n'
  '\treport_payload JSON, \n'
  '\treport_ack JSON, \n'
  '\tPRIMARY KEY (id), \n'
  '\tCONSTRAINT unique_job_attempt UNIQUE (job_id, number), \n'
  '\tCONSTRAINT positive_attempt_number CHECK (number > 0), \n'
  '\tCONSTRAINT valid_attempt_state CHECK (state IN '
  "('assigned','claimed','completed','failed','expired')), \n"
  '\tCONSTRAINT coherent_attempt_state CHECK (\n'
  "            (state = 'assigned' AND claim_id IS NULL AND claimed_at IS NULL AND finished_at IS "
  'NULL)\n'
  "            OR (state = 'claimed' AND claim_id IS NOT NULL AND claimed_at IS NOT NULL AND "
  'finished_at IS NULL)\n'
  "            OR (state IN ('completed','failed') AND claim_id IS NOT NULL AND claimed_at IS NOT "
  'NULL AND finished_at IS NOT NULL)\n'
  "            OR (state = 'expired' AND finished_at IS NOT NULL)\n"
  '        ), \n'
  '\tFOREIGN KEY(job_id) REFERENCES jobs (id), \n'
  '\tFOREIGN KEY(worker_id) REFERENCES workers (id), \n'
  '\tUNIQUE (claim_id)\n'
  ')'),
 ('table',
  'job_renewals',
  'job_renewals',
  'CREATE TABLE job_renewals (\n'
  '\tid INTEGER NOT NULL, \n'
  '\tattempt_id INTEGER NOT NULL, \n'
  '\trenewal_id VARCHAR(36) NOT NULL, \n'
  '\tgranted_at DATETIME NOT NULL, \n'
  '\tlease_expires_at DATETIME NOT NULL, \n'
  '\tPRIMARY KEY (id), \n'
  '\tUNIQUE (attempt_id, renewal_id), \n'
  '\tFOREIGN KEY(attempt_id) REFERENCES job_attempts (id)\n'
  ')'),
 ('table',
  'jobs',
  'jobs',
  'CREATE TABLE jobs (\n'
  '\tid INTEGER NOT NULL, \n'
  '\tdeployment_id INTEGER NOT NULL, \n'
  '\tworker_id INTEGER, \n'
  '\tkind VARCHAR(30) NOT NULL, \n'
  '\tstate VARCHAR(20) NOT NULL, \n'
  '\tcreated_at DATETIME NOT NULL, \n'
  '\tassigned_at DATETIME, \n'
  '\tclaimed_at DATETIME, \n'
  '\tfinished_at DATETIME, \n'
  '\tclaim_id VARCHAR(36), \n'
  '\tresult JSON, \n'
  '\terror VARCHAR(2000), \n'
  '\tattempt_count INTEGER NOT NULL, \n'
  '\tmax_attempts INTEGER NOT NULL, \n'
  '\tnext_run_at DATETIME NOT NULL, \n'
  '\tassignment_expires_at DATETIME, \n'
  '\tlease_expires_at DATETIME, \n'
  '\tlast_failure_code VARCHAR(50), \n'
  '\tPRIMARY KEY (id), \n'
  "\tCONSTRAINT valid_job_kind CHECK (kind = 'prepare_deployment'), \n"
  "\tCONSTRAINT valid_job_state CHECK (state IN ('queued', 'assigned', 'claimed', 'completed', "
  "'failed')), \n"
  '\tCONSTRAINT coherent_job_ownership CHECK (\n'
  "            (state = 'queued' AND worker_id IS NULL AND assigned_at IS NULL\n"
  '                AND claim_id IS NULL AND claimed_at IS NULL AND finished_at IS NULL)\n'
  "            OR (state = 'assigned' AND worker_id IS NOT NULL AND assigned_at IS NOT NULL\n"
  '                AND claim_id IS NULL AND claimed_at IS NULL AND finished_at IS NULL)\n'
  "            OR (state = 'claimed' AND worker_id IS NOT NULL AND assigned_at IS NOT NULL\n"
  '                AND claim_id IS NOT NULL AND claimed_at IS NOT NULL AND finished_at IS NULL)\n'
  "            OR (state = 'completed' AND worker_id IS NOT NULL\n"
  '                AND assigned_at IS NOT NULL AND claim_id IS NOT NULL\n'
  '                AND claimed_at IS NOT NULL AND finished_at IS NOT NULL)\n'
  "            OR (state = 'failed' AND finished_at IS NOT NULL)\n"
  '        ), \n'
  '\tCONSTRAINT bounded_job_attempts CHECK (attempt_count >= 0 AND max_attempts > 0 AND '
  'attempt_count <= max_attempts), \n'
  '\tCONSTRAINT coherent_job_deadlines CHECK (\n'
  "            (state = 'assigned' AND assignment_expires_at IS NOT NULL AND lease_expires_at IS "
  'NULL)\n'
  "            OR (state = 'claimed' AND assignment_expires_at IS NULL AND lease_expires_at IS NOT "
  'NULL)\n'
  "            OR (state IN ('queued', 'completed', 'failed') AND assignment_expires_at IS NULL "
  'AND lease_expires_at IS NULL)\n'
  '        ), \n'
  "\tCONSTRAINT job_error_only_on_failure CHECK (error IS NULL OR state = 'failed'), \n"
  "\tCONSTRAINT failed_job_has_error CHECK (state != 'failed' OR error IS NOT NULL), \n"
  "\tCONSTRAINT job_result_only_on_completion CHECK (result IS NULL OR state = 'completed'), \n"
  '\tUNIQUE (deployment_id), \n'
  '\tFOREIGN KEY(deployment_id) REFERENCES deployments (id), \n'
  '\tFOREIGN KEY(worker_id) REFERENCES workers (id), \n'
  '\tUNIQUE (claim_id)\n'
  ')'),
 ('table',
  'workers',
  'workers',
  'CREATE TABLE workers (\n'
  '\tid INTEGER NOT NULL, \n'
  '\tidentity VARCHAR(100) NOT NULL, \n'
  '\tregistered_at DATETIME NOT NULL, \n'
  '\tlast_heartbeat_at DATETIME NOT NULL, \n'
  '\tPRIMARY KEY (id), \n'
  '\tUNIQUE (identity)\n'
  ')'),
 ('index',
  'ix_job_attempts_job_id',
  'job_attempts',
  'CREATE INDEX ix_job_attempts_job_id ON job_attempts (job_id)'),
 ('index',
  'ix_job_renewals_attempt_id',
  'job_renewals',
  'CREATE INDEX ix_job_renewals_attempt_id ON job_renewals (attempt_id)'),
 ('index',
  'jobs_assignment_expiry',
  'jobs',
  'CREATE INDEX jobs_assignment_expiry ON jobs (state, assignment_expires_at)'),
 ('index',
  'jobs_lease_expiry',
  'jobs',
  'CREATE INDEX jobs_lease_expiry ON jobs (state, lease_expires_at)'),
 ('index',
  'jobs_queue_order',
  'jobs',
  'CREATE INDEX jobs_queue_order ON jobs (state, created_at, id)'),
 ('index', 'jobs_ready', 'jobs', 'CREATE INDEX jobs_ready ON jobs (state, next_run_at)'),
 ('index',
  'one_active_job_per_worker',
  'jobs',
  "CREATE UNIQUE INDEX one_active_job_per_worker ON jobs (worker_id) WHERE state IN ('assigned', "
  "'claimed')"),
 ('trigger',
  'attempts_valid_transition',
  'job_attempts',
  'CREATE TRIGGER attempts_valid_transition BEFORE UPDATE ON job_attempts\n'
  '    WHEN NEW.job_id != OLD.job_id OR NEW.number != OLD.number OR NEW.worker_id != '
  'OLD.worker_id\n'
  '      OR NEW.assigned_at != OLD.assigned_at\n'
  '      OR (OLD.claim_id IS NOT NULL AND NEW.claim_id IS NOT OLD.claim_id)\n'
  "      OR (OLD.state IN ('completed','failed','expired'))\n"
  '      OR (NEW.state != OLD.state AND NOT (\n'
  "        (OLD.state='assigned' AND NEW.state IN ('claimed','expired')) OR\n"
  "        (OLD.state='claimed' AND NEW.state IN ('completed','failed','expired'))\n"
  '      ))\n'
  "    BEGIN SELECT RAISE(ABORT, 'invalid attempt mutation'); END"),
 ('trigger',
  'jobs_valid_transition',
  'jobs',
  'CREATE TRIGGER jobs_valid_transition\n'
  '        BEFORE UPDATE OF state ON jobs\n'
  '        WHEN NEW.state IS NOT OLD.state AND NOT (\n'
  "            (OLD.state = 'queued' AND NEW.state = 'assigned') OR\n"
  "            (OLD.state = 'assigned' AND NEW.state IN ('claimed', 'queued', 'failed')) OR\n"
  "            (OLD.state = 'claimed' AND NEW.state IN ('completed', 'failed', 'queued'))\n"
  '        )\n'
  '        BEGIN\n'
  "            SELECT RAISE(ABORT, 'invalid job state transition');\n"
  '        END')]

def create_table(connection, name):
    for kind, _, table, sql in SCHEMA:
        if table == name:
            connection.exec_driver_sql(sql)
