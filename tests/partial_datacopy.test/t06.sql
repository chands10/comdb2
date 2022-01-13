create table t {
    schema {
        int a
        int b
        int c
        int d
    }
    keys {
        datacopy(a, b, c) "ac" = a + c
    }
};$$


select * from comdb2_keys where tablename='t';
select * from comdb2_partial_datacopies where tablename='t';

drop table t;
