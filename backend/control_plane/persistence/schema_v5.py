"""Frozen version 5 tables changed by later migrations."""

DDL = {'deployment_targets': ['CREATE TABLE deployment_targets (\n'
                        '\tdeployment_id INTEGER NOT NULL, \n'
                        '\tdesired_replicas INTEGER NOT NULL, \n'
                        '\tversion INTEGER NOT NULL, \n'
                        '\tcreated_at DATETIME NOT NULL, \n'
                        '\tupdated_at DATETIME NOT NULL, \n'
                        '\tPRIMARY KEY (deployment_id), \n'
                        '\tCONSTRAINT bounded_replica_target CHECK (typeof(desired_replicas) = '
                        "'integer' AND desired_replicas BETWEEN 0 AND 100), \n"
                        '\tCONSTRAINT positive_target_version CHECK (version > 0), \n'
                        '\tFOREIGN KEY(deployment_id) REFERENCES deployments (id)\n'
                        ')',
                        'CREATE TRIGGER targets_valid_update BEFORE UPDATE ON deployment_targets\n'
                        '    WHEN NEW.deployment_id != OLD.deployment_id OR NEW.created_at != '
                        'OLD.created_at\n'
                        '      OR NEW.version != OLD.version + 1 OR NEW.desired_replicas <= '
                        'OLD.desired_replicas\n'
                        "    BEGIN SELECT RAISE(ABORT, 'invalid replica target mutation'); END"],
 'instances': ['CREATE TABLE instances (\n'
               '\tid VARCHAR(36) NOT NULL, \n'
               '\tdeployment_id INTEGER NOT NULL, \n'
               '\treplica_index INTEGER NOT NULL, \n'
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
               '\tCONSTRAINT unique_replica_slot UNIQUE (deployment_id, replica_index), \n'
               "\tCONSTRAINT valid_replica_index CHECK (typeof(replica_index) = 'integer' AND "
               'replica_index >= 0), \n'
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
               '\tFOREIGN KEY(deployment_id) REFERENCES deployments (id), \n'
               '\tUNIQUE (start_job_id), \n'
               '\tFOREIGN KEY(start_job_id) REFERENCES jobs (id), \n'
               '\tFOREIGN KEY(worker_id) REFERENCES workers (id), \n'
               '\tFOREIGN KEY(last_attempt_id) REFERENCES job_attempts (id)\n'
               ')',
               'CREATE TRIGGER instances_valid_transition BEFORE UPDATE ON instances\n'
               '    WHEN NEW.id != OLD.id OR NEW.deployment_id != OLD.deployment_id OR '
               'NEW.start_job_id != OLD.start_job_id\n'
               '      OR NEW.replica_index != OLD.replica_index\n'
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
