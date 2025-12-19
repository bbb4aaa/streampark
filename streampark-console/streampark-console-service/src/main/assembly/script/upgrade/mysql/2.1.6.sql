alter table t_flink_app
    add column(restart_savepoint_recovery tinyint(1) default 1 null comment 'Restart Savepoint Recovery');
