#!/usr/bin/env ./tcltestrunner.lua

# 2006 January 20
#
# The author disclaims copyright to this source code.  In place of
# a legal notice, here is a blessing:
#
#    May you do good and not evil.
#    May you find forgiveness for yourself and forgive others.
#    May you share freely, never taking more than you give.
#
#***********************************************************************
# This file implements regression tests for SQLite library.
#
# This file implements tests for calling sqlite3_result_error()
# from within an aggregate function implementation.
#
# $Id: aggerror.test,v 1.3 2006/05/03 23:34:06 drh Exp $

set testdir [file dirname $argv0]
source $testdir/tester.tcl


# Add the x_count aggregate function to the database handle.
# x_count will error out if its input is 40 or 41 or if its
# final results is 42.  Make sure that such errors are handled
# appropriately.
#
do_test aggerror-1.1 {
  set DB [sqlite3_connection_pointer db]
  sqlite3_create_aggregate $DB
  execsql {
    CREATE TABLE t1(a integer primary key);
    INSERT INTO t1 VALUES(1);
    INSERT INTO t1 VALUES(2);
    INSERT INTO t1 VALUES(3);
    INSERT INTO t1 VALUES(4);
    INSERT INTO t1 VALUES(5);
    INSERT INTO t1 VALUES(6);
    INSERT INTO t1 VALUES(7);
    INSERT INTO t1 VALUES(8);
    INSERT INTO t1 VALUES(9);
    INSERT INTO t1 VALUES(10);
    INSERT INTO t1 VALUES(11);
    INSERT INTO t1 VALUES(12);
    INSERT INTO t1 VALUES(13);
    INSERT INTO t1 VALUES(14);
    INSERT INTO t1 VALUES(15);
    INSERT INTO t1 VALUES(16);
    INSERT INTO t1 VALUES(17);
    INSERT INTO t1 VALUES(18);
    INSERT INTO t1 VALUES(19);
    INSERT INTO t1 VALUES(20);
    INSERT INTO t1 VALUES(21);
    INSERT INTO t1 VALUES(22);
    INSERT INTO t1 VALUES(23);
    INSERT INTO t1 VALUES(24);
    INSERT INTO t1 VALUES(25);
    INSERT INTO t1 VALUES(26);
    INSERT INTO t1 VALUES(27);
    INSERT INTO t1 VALUES(28);
    INSERT INTO t1 VALUES(29);
    INSERT INTO t1 VALUES(30);
    INSERT INTO t1 VALUES(31);
    INSERT INTO t1 VALUES(32);
    INSERT INTO t1 VALUES(33);
    INSERT INTO t1 VALUES(34);
    INSERT INTO t1 VALUES(35);
    INSERT INTO t1 VALUES(36);
    INSERT INTO t1 VALUES(37);
    INSERT INTO t1 VALUES(38);
    INSERT INTO t1 VALUES(39);
    SELECT x_count(*) FROM t1;
  }
} {39}
do_test aggerror-1.2 {
  execsql {
    INSERT INTO t1 VALUES(40);
    SELECT x_count(*) FROM t1;
  }
} {40}
do_test aggerror-1.3 {
  catchsql {
    SELECT x_count(a) FROM t1;
  }
} {1 {value of 40 handed to x_count}}
ifcapable utf16 {
  do_test aggerror-1.4 {
    execsql {
      UPDATE t1 SET a=41 WHERE a=40
    }
    catchsql {
      SELECT x_count(a) FROM t1;
    }
  } {1 abc}
}
do_test aggerror-1.5 {
  execsql {
    SELECT x_count(*) FROM t1
  }
} 40
do_test aggerror-1.6 {
  execsql {
    INSERT INTO t1 VALUES(40);
    INSERT INTO t1 VALUES(42);
  }
  catchsql {
    SELECT x_count(*) FROM t1;
  }
} {1 {x_count totals to 42}}

finish_test
