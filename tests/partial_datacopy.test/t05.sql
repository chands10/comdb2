create table t {
    schema {
        int a
        int b
        int c
        int d
    }
    keys {
        datacopy(a) "a" = a
    }
};$$

create table t2 {
    schema {
        int a
        int b
        int c
        int d
    }
    keys {
        datacopy(a, b, c, d) "abcd" = a + b + c + d
    }
};$$

select * from comdb2_keys where tablename='t';
select * from comdb2_partial_datacopies where tablename='t';

select * from comdb2_keys where tablename='t2';
select * from comdb2_partial_datacopies where tablename='t2';

drop table t;
drop table t2;
