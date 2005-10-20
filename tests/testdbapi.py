#!/usr/bin/env python
import dbapi20
import unittest
import informixdb
import popen2

class TestDBAPI(dbapi20.DatabaseAPI20Test):
    driver = informixdb
    connect_args = ('ifxtest',)
    connect_kw_args = {}

    lower_func = 'db20t_lower' # For stored procedure test

    table_prefix = dbapi20.DatabaseAPI20Test.table_prefix

    iddl1 = """create table %sdate (a date,
                                  b datetime hour to fraction(5),
                                  c datetime day to minute)""" \
             % table_prefix
    xiddl1 = 'drop table %sdate' % table_prefix

    storedproc = \
    """create procedure %slower ( str1 CHAR(5) )
          returning CHAR(5);

            if str1 = 'FOO' then
                return 'foo';
            else
                return 'oops';
            end if;
       end procedure;
    """ % table_prefix
    xstoredproc = "drop procedure %slower" % table_prefix

    def setUp(self):
        dbapi20.DatabaseAPI20Test.setUp(self)

        try:
            con = self._connect()
            con.close()
        except:
            cmd = "dbaccess"
            cout,cin = popen2.popen2(cmd)
            cin.write('create database ifxtest')
            cin.close()
            cout.read()

        con = self._connect()
        try:
            c = con.cursor()
            try:
                c.execute(self.xstoredproc)
            except:
                pass
            c.execute(self.storedproc)
            con.commit()
        finally:
            con.close()

    def test_error(self):
        con = self._connect()
        try:
            try:
                c = con.cursor()
                c.execute("SELECT * MORF SYNTAXERROR");
            except self.driver.DatabaseError, e:
                # this are not really whitebox tests anymore, but handy
                # anyway
                self.assertEqual(e.sqlcode, -201,
                                 "Wrong or no SQLCODE set");
                self.assertEqual(e.action, "PREPARE",
                                 "Wrong or no ACTION set");
                self.assertEqual(len(e.diagnostics), 1,
                                 "Wrong or no DIAGNOSTICS set");
            else:
                self.fail("Should raise DatabaseError on invalid Operation")
        finally:
            con.close()

    # ripped from test_fetchone()
    def test_iterator(self):
        con = self._connect()
        try:
            cur = con.cursor()

            it = iter(cur)

            # it.next() should raise an Error if called before
            # executing a select-type query
            self.assertRaises(self.driver.Error, it.next)

            # it.fetchone should raise an Error if called after
            # executing a query that cannnot return rows
            self.executeDDL1(cur)
            self.assertRaises(self.driver.Error, it.next)

            # it.fetchone should raise an Error if called after
            # executing a query that cannnot return rows
            cur.execute("insert into %sbooze values ('Victoria Bitter')" % (
                self.table_prefix
                ))
            self.assertRaises(self.driver.Error,it.next)

            cur.execute('select name from %sbooze' % self.table_prefix)
            count = 0
            for r in cur:
                count += 1
                self.assertEqual(r[0],'Victoria Bitter',
                    'iter(cursor).next retrieved incorrect data'
                    )

            self.assertEqual(count, 1,
                'iter(cursor).next should have retrieved a single row'
                )
        finally:
            con.close()

    def test_datetime(self):
        con = self._connect()
        try:
            cur = con.cursor()
            cur.execute(self.iddl1)

            a = self.driver.Date(2005,9,30)
            b = self.driver.Time(21,34,18,123450)
            c = self.driver.Timestamp(1,1,30,21,34,0)

            cur.execute("INSERT INTO %sdate VALUES (?,?,?)" % self.table_prefix,
                        (a, b, c))

            cur.execute("SELECT * FROM %sdate" % self.table_prefix)
            vals = cur.fetchone()

            self.assertEqual(vals[0], a, 'Read back values of Date differ')
            self.assertEqual(vals[1], b, 'Read back values of Time differ')
            self.assertEqual(vals[2], c, 'Read back values of Timestamp differ')
        finally:
            con.close()

    def test_named_cursor(self):
        con = self._connect()
        try:
            cur = con.cursor('db20tupd')
            cur2 = con.cursor()

            self.executeDDL1(cur)

            for sql in self._populate():
                cur.execute(sql)

            cur.execute('select name from %sbooze for update' %
                        self.table_prefix)
            for r in cur:
                if (r[0] == 'Victoria Bitter'):
                    cur2.execute("""update %sbooze set name = ? where
                                   current of db20tupd""" % self.table_prefix,
                                ('Nobody',))

        finally:
            con.close()

    def test_dict_cursor(self):
        con = self._connect()
        try:
            cur = con.cursor(rowformat = informixdb.ROW_AS_DICT)

            self.executeDDL1(cur)

            cur.execute("insert into %sbooze values ('Victoria Bitter')" %
                        self.table_prefix)
            cur.execute('select name from %sbooze' % self.table_prefix)

            r = cur.fetchone()
            self.assertEqual(r['name'], 'Victoria Bitter')

        finally:
            con.close()

    def test_dbapitype_richcmp(self):
         # test for regression of != comparison of DBAPIType objects
         self.assertEqual(self.driver.STRING, 'char',
                          "DBAPIType comparison broken")
         self.assertNotEqual(self.driver.STRING, 'something',
                          "DBAPIType comparison broken")

    def tearDown(self):
        dbapi20.DatabaseAPI20Test.tearDown(self)

        con = self._connect()
        try:
            cur = con.cursor()
            for ddl in (self.xiddl1,self.xstoredproc,):
                try:
                    cur.execute(ddl)
                    con.commit()
                except self.driver.Error:
                    pass
        finally:
            con.close()

    def test_nextset(self): pass
    def test_setoutputsize(self): pass

if __name__ == '__main__':
    unittest.main()
