create table t {
    schema {
        int a
        int b
        int c
        int d
    }
    keys {
        datacopy(b, b, b, b, c, c, c, c, b, c, b, c, b, d) "a" = a
    }
};$$

select * from comdb2_keys where tablename='t';

select * from comdb2_partial_datacopies where tablename='t';

drop table t;
