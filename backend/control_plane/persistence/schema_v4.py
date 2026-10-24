"""Frozen version 4 DDL; historical migrations never use current table models."""

DDL = {'jobs': ['CREATE TABLE jobs (\n'
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
          "\tCONSTRAINT valid_job_kind CHECK (kind IN ('prepare_deployment', 'start_instance')), \n"
          "\tCONSTRAINT valid_job_state CHECK (state IN ('queued', 'assigned', 'claimed', "
          "'completed', 'failed')), \n"
          '\tCONSTRAINT coherent_job_ownership CHECK (\n'
          "            (state = 'queued' AND worker_id IS NULL AND assigned_at IS NULL\n"
          '                AND claim_id IS NULL AND claimed_at IS NULL AND finished_at IS NULL)\n'
          "            OR (state = 'assigned' AND worker_id IS NOT NULL AND assigned_at IS NOT "
          'NULL\n'
          '                AND claim_id IS NULL AND claimed_at IS NULL AND finished_at IS NULL)\n'
          "            OR (state = 'claimed' AND worker_id IS NOT NULL AND assigned_at IS NOT "
          'NULL\n'
          '                AND claim_id IS NOT NULL AND claimed_at IS NOT NULL AND finished_at IS '
          'NULL)\n'
          "            OR (state = 'completed' AND worker_id IS NOT NULL\n"
          '                AND assigned_at IS NOT NULL AND claim_id IS NOT NULL\n'
          '                AND claimed_at IS NOT NULL AND finished_at IS NOT NULL)\n'
          "            OR (state = 'failed' AND finished_at IS NOT NULL)\n"
          '        ), \n'
          '\tCONSTRAINT bounded_job_attempts CHECK (attempt_count >= 0 AND max_attempts > 0 AND '
          'attempt_count <= max_attempts), \n'
          '\tCONSTRAINT coherent_job_deadlines CHECK (\n'
          "            (state = 'assigned' AND assignment_expires_at IS NOT NULL AND "
          'lease_expires_at IS NULL)\n'
          "            OR (state = 'claimed' AND assignment_expires_at IS NULL AND "
          'lease_expires_at IS NOT NULL)\n'
          "            OR (state IN ('queued', 'completed', 'failed') AND assignment_expires_at IS "
          'NULL AND lease_expires_at IS NULL)\n'
          '        ), \n'
          "\tCONSTRAINT job_error_only_on_failure CHECK (error IS NULL OR state = 'failed'), \n"
          "\tCONSTRAINT failed_job_has_error CHECK (state != 'failed' OR error IS NOT NULL), \n"
          '\tCONSTRAINT job_result_only_on_completion CHECK (result IS NULL OR state = '
          "'completed'), \n"
          '\tFOREIGN KEY(deployment_id) REFERENCES deployments (id), \n'
          '\tFOREIGN KEY(worker_id) REFERENCES workers (id), \n'
          '\tUNIQUE (claim_id)\n'
          ')',
          'CREATE INDEX jobs_queue_order ON jobs (state, created_at, id)',
          'CREATE UNIQUE INDEX one_active_job_per_worker ON jobs (worker_id) WHERE state IN '
          "('assigned', 'claimed')",
          'CREATE UNIQUE INDEX one_prepare_job_per_deployment ON jobs (deployment_id) WHERE kind = '
          "'prepare_deployment'",
          'CREATE INDEX jobs_ready ON jobs (state, next_run_at)',
          'CREATE INDEX jobs_lease_expiry ON jobs (state, lease_expires_at)',
          'CREATE INDEX jobs_assignment_expiry ON jobs (state, assignment_expires_at)',
          'CREATE TRIGGER jobs_valid_transition\n'
          '        BEFORE UPDATE OF state ON jobs\n'
          '        WHEN NEW.state IS NOT OLD.state AND NOT (\n'
          "            (OLD.state = 'queued' AND NEW.state = 'assigned') OR\n"
          "            (OLD.state = 'assigned' AND NEW.state IN ('claimed', 'queued', 'failed')) "
          'OR\n'
          "            (OLD.state = 'claimed' AND NEW.state IN ('completed', 'failed', 'queued'))\n"
          '        )\n'
          '        BEGIN\n'
          "            SELECT RAISE(ABORT, 'invalid job state transition');\n"
          '        END'],
 'job_attempts': ['CREATE TABLE job_attempts (\n'
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
                  "            (state = 'assigned' AND claim_id IS NULL AND claimed_at IS NULL AND "
                  'finished_at IS NULL)\n'
                  "            OR (state = 'claimed' AND claim_id IS NOT NULL AND claimed_at IS "
                  'NOT NULL AND finished_at IS NULL)\n'
                  "            OR (state IN ('completed','failed') AND claim_id IS NOT NULL AND "
                  'claimed_at IS NOT NULL AND finished_at IS NOT NULL)\n'
                  "            OR (state = 'expired' AND finished_at IS NOT NULL)\n"
                  '        ), \n'
                  '\tFOREIGN KEY(job_id) REFERENCES jobs (id), \n'
                  '\tFOREIGN KEY(worker_id) REFERENCES workers (id), \n'
                  '\tUNIQUE (claim_id)\n'
                  ')',
                  'CREATE INDEX ix_job_attempts_job_id ON job_attempts (job_id)',
                  'CREATE TRIGGER attempts_valid_transition BEFORE UPDATE ON job_attempts\n'
                  '    WHEN NEW.job_id != OLD.job_id OR NEW.number != OLD.number OR NEW.worker_id '
                  '!= OLD.worker_id\n'
                  '      OR NEW.assigned_at != OLD.assigned_at\n'
                  '      OR (OLD.claim_id IS NOT NULL AND NEW.claim_id IS NOT OLD.claim_id)\n'
                  "      OR (OLD.state IN ('completed','failed','expired'))\n"
                  '      OR (NEW.state != OLD.state AND NOT (\n'
                  "        (OLD.state='assigned' AND NEW.state IN ('claimed','expired')) OR\n"
                  "        (OLD.state='claimed' AND NEW.state IN "
                  "('completed','failed','expired'))\n"
                  '      ))\n'
                  "    BEGIN SELECT RAISE(ABORT, 'invalid attempt mutation'); END"],
 'job_renewals': ['CREATE TABLE job_renewals (\n'
                  '\tid INTEGER NOT NULL, \n'
                  '\tattempt_id INTEGER NOT NULL, \n'
                  '\trenewal_id VARCHAR(36) NOT NULL, \n'
                  '\tgranted_at DATETIME NOT NULL, \n'
                  '\tlease_expires_at DATETIME NOT NULL, \n'
                  '\tPRIMARY KEY (id), \n'
                  '\tUNIQUE (attempt_id, renewal_id), \n'
                  '\tFOREIGN KEY(attempt_id) REFERENCES job_attempts (id)\n'
                  ')',
                  'CREATE INDEX ix_job_renewals_attempt_id ON job_renewals (attempt_id)'],
 'instances': ['CREATE TABLE instances (\n'
               '\tid VARCHAR(36) NOT NULL, \n'
               '\tdeployment_id INTEGER NOT NULL, \n'
               '\tstart_job_id INTEGER NOT NULL, \n'
               '\tworker_id INTEGER, \n'
               '\tlast_attempt_id INTEGER, \n'
               '\tstate VARCHAR(20) NOT NULL, \n'
               '\tversion INTEGER NOT NULL, \n'
               '\tcreated_at DATETIME NOT NULL, \n'
               '\tupdated_at DATETIME NOT NULL, \n'
               '\tlast_observed_at DATETIME, \n'
               '\thealthy_at DATETIME, \n'
               '\tstopped_at DATETIME, \n'
               '\tstate_source VARCHAR(20) NOT NULL, \n'
               '\tstate_reason VARCHAR(100) NOT NULL, \n'
               '\tPRIMARY KEY (id), \n'
               '\tCONSTRAINT valid_instance_state CHECK (state IN '
               "('pending','starting','healthy','failed','stopped')), \n"
               '\tCONSTRAINT positive_instance_version CHECK (version > 0), \n'
               '\tCONSTRAINT valid_instance_source CHECK (state_source IN '
               "('control_plane','worker_report','manual')), \n"
               "\tCONSTRAINT healthy_instance_observed CHECK (state != 'healthy' OR (worker_id IS "
               'NOT NULL AND last_attempt_id IS NOT NULL AND healthy_at IS NOT NULL AND '
               'last_observed_at IS NOT NULL)), \n'
               "\tCONSTRAINT coherent_instance_stop CHECK ((state = 'stopped') = (stopped_at IS "
               'NOT NULL)), \n'
               '\tUNIQUE (deployment_id), \n'
               '\tFOREIGN KEY(deployment_id) REFERENCES deployments (id), \n'
               '\tUNIQUE (start_job_id), \n'
               '\tFOREIGN KEY(start_job_id) REFERENCES jobs (id), \n'
               '\tFOREIGN KEY(worker_id) REFERENCES workers (id), \n'
               '\tFOREIGN KEY(last_attempt_id) REFERENCES job_attempts (id)\n'
               ')',
               'CREATE TRIGGER instances_valid_transition BEFORE UPDATE ON instances\n'
               '    WHEN NEW.id != OLD.id OR NEW.deployment_id != OLD.deployment_id OR '
               'NEW.start_job_id != OLD.start_job_id\n'
               '      OR NEW.created_at != OLD.created_at OR NEW.version != OLD.version + 1\n'
               '      OR (NEW.state != OLD.state AND NOT (\n'
               "        (OLD.state='pending' AND NEW.state IN ('starting','failed')) OR\n"
               "        (OLD.state='starting' AND NEW.state IN ('healthy','failed')) OR\n"
               "        (OLD.state='failed' AND NEW.state IN ('starting','stopped')) OR\n"
               "        (OLD.state='healthy' AND NEW.state='stopped')\n"
               "      )) OR OLD.state='stopped'\n"
               "    BEGIN SELECT RAISE(ABORT, 'invalid instance mutation'); END"]}


def create_table(connection, name):
    for statement in DDL[name]:
        connection.exec_driver_sql(statement)
