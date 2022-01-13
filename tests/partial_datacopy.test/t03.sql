create table t {
    schema {
        int a
        int b
        int c
        int d
    }
    keys {
        datacopy(b, c) "a" = a
    }
};$$

select * from comdb2_keys where tablename='t';

select * from comdb2_partial_datacopies where tablename='t';

drop table t;
