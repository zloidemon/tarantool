#!/usr/bin/env ./tcltestrunner.lua

# 2009 April 24                                                                 
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
#
# Ticket #3824
#
# When you use an "IS NULL" constraint on a UNIQUE index, the result
# is not necessarily UNIQUE.  Make sure the optimizer does not assume
# uniqueness.
#
# $Id: tkt3824.test,v 1.2 2009/04/24 20:32:31 drh Exp $

set testdir [file dirname $argv0]
source $testdir/tester.tcl

proc execsql_status {sql {db db}} {
  set result [uplevel $db eval [list $sql]]
  if {[db status sort]} {
    concat $result sort
  } else {
    concat $result nosort
  }
}

do_test tkt3824-1.1 {
  db eval {
    CREATE TABLE t1(id primary key, a,b);
    INSERT INTO t1 VALUES(1,1,NULL);
    INSERT INTO t1 VALUES(2,9,NULL);
    INSERT INTO t1 VALUES(3,5,NULL);
    INSERT INTO t1 VALUES(4,123,NULL);
    INSERT INTO t1 VALUES(5,-10,NULL);
    CREATE UNIQUE INDEX t1b ON t1(b);
  }
  execsql_status {
    SELECT a FROM t1 WHERE b IS NULL ORDER BY a;
  }
} {-10 1 5 9 123 sort}
do_test tkt3824-1.2 {
  execsql_status {
    SELECT a FROM t1 WHERE b IS NULL ORDER BY b, a;
  }
} {-10 1 5 9 123 sort}

do_test tkt3824-2.1 {
  db eval {
    CREATE TABLE t2(id primary key, a,b,c);
    INSERT INTO t2 VALUES(1,1,1,NULL);
    INSERT INTO t2 VALUES(2,9,2,NULL);
    INSERT INTO t2 VALUES(3,5,2,NULL);
    INSERT INTO t2 VALUES(4,123,3,NULL);
    INSERT INTO t2 VALUES(5,-10,3,NULL);
    CREATE UNIQUE INDEX t2bc ON t2(b,c);
  }
  execsql_status {
    SELECT a FROM t2 WHERE b=2 AND c IS NULL ORDER BY a;
  }
} {5 9 sort}
do_test tkt3824-2.2 {
  execsql_status {
    SELECT a FROM t2 WHERE b=2 AND c IS NULL ORDER BY b, a;
  }
} {5 9 sort}
do_test tkt3824-2.3 {
  lsort [execsql_status {
    SELECT a FROM t2 WHERE b=2 AND c IS NULL ORDER BY b;
  }]
} {5 9 nosort}

do_test tkt3824-3.1 {
  db eval {
    CREATE TABLE t3(id primary key,x,y);
    INSERT INTO t3 SELECT id, a, b FROM t1;
    INSERT INTO t3 VALUES(6, 234,567);
    CREATE UNIQUE INDEX t3y ON t3(y);
    DELETE FROM t3 WHERE y IS NULL;
    SELECT x,y FROM t3;
  }
} {234 567}

do_test tkt3824-4.1 {
  db eval {
    CREATE TABLE t4(id primary key, x,y);
    INSERT INTO t4 SELECT id, a, b FROM t1;
    INSERT INTO t4 VALUES(6, 234,567);
    CREATE UNIQUE INDEX t4y ON t4(y);
    UPDATE t4 SET id=id+100 WHERE y IS NULL;
    SELECT id, x FROM t4 ORDER BY id;
  }
} {6 234 101 1 102 9 103 5 104 123 105 -10}

finish_test
