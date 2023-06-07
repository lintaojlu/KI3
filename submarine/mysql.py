import pymysql


class MySQLDatabase():
    """
    Whenever sending a SQL to MySQL server, use conn_acquire and conn_release to 
    establish and close a new connection exclusively used for this SQL execution.
    This can avoid waiting timeout problem caused by MySQL's default setting.
    """

    g17_DB_HOST = '219.243.215.203'
    g17_PORT = 23306
    g17_USER = 'root'
    g17_PWD = '3LxsXc!*K5A^4W#uq!Y1'

    k01_DB_HOST = '166.111.121.63'
    k01_PORT = 23306
    k01_USER = 'ki3-backends'
    k01_PWD = 'Backends12345@'

    k01_DB_HOST_TEST = '166.111.121.63'
    k01_PORT_TEST = 23307
    k01_USER_TEST = 'ki3-backends'
    k01_PWD_TEST = 'Backends12345@' 

    def __init__(self):
        self.mysql_conn = None
    
    def conn_acquire(self):
        self.mysql_conn = pymysql.connect(host=self.k01_DB_HOST_TEST, port=self.k01_PORT_TEST, user=self.k01_USER_TEST,
                                         password=self.k01_PWD_TEST, db='website')

        # set max_allowed_packet to 1G
        # with self.mysql_conn.cursor() as cursor:
        #     cursor.execute('set global max_allowed_packet=1073741824;')
        # self.mysql_conn.commit()
    
    def conn_release(self):
        self.mysql_conn.close()

    def execute(self, sql):
        """
        Execute the given sql.
        """

        self.conn_acquire()

        try:
            with self.mysql_conn.cursor() as cursor:
                cursor.execute(sql)
            self.mysql_conn.commit()
        except Exception as e:
            try:
                self.mysql_conn.rollback()
            except Exception:
                pass
            self.conn_release()
            return False, '[ERROR] {}'.format(str(e))
        
        self.conn_release()
        return True, None
    
    def fetch(self, sql):
        """
        Execute the given sql and fetch results from db.
        """

        self.conn_acquire()

        select_results = set()
        try:
            with self.mysql_conn.cursor() as cursor:
                cursor.execute(sql)
                select_results = cursor.fetchall()
        except Exception as e:
            self.conn_release()
            return False, '[ERROR] {}'.format(str(e))

        self.conn_release()
        return True, select_results
    
    def get_column_names(self, table):
        """
        Get column names of the given table.
        """

        column_list = list()
        query_sql = ("SELECT COLUMN_NAME "
                     "FROM information_schema.COLUMNS "
                     "WHERE table_schema='website' AND table_name='{}'".format(table))
        
        _, resp = self.fetch(query_sql)
        for tuple in resp:
            column_list.append(tuple[0])
        
        return column_list
    
    def single_update(self, table, res_list, col_list=None):
        col_list = self.get_column_names(table)

        sql = 'SELECT * FROM {} WHERE {} = "{}";'.format(table, col_list[0], res_list[0])
        succ, resp = self.fetch(sql)
        if not succ:
            return succ, resp
        if len(resp) == 0:
            self.single_insert(table, res_list)
            return True, None
        # Update
        set_list = ''
        for i in range(1, len(res_list)):
            set_list += '{}="{}", '.format(col_list[i], res_list[i])
        set_list = set_list[:-2]
        sql = 'UPDATE {} SET {} WHERE {} = "{}";'.format(table, set_list, col_list[0], res_list[0])
        succ, resp = self.execute(sql)
        return succ, resp
    
    def single_insert(self, table, res_list, col_list=None):
        return self.batch_insert(table, [res_list,], col_list)
    
    def batch_insert(self, table, res_list, col_list=None):
        """
        Insert batch records to db
        
        :param table:       the name of table to perform INSERT
        :param res_list:    the 2d list of VALUES to INSERT
        :param col_list:    the list of column names to perform INSERT
                            None if all columns are used
        """

        if len(res_list) == 0:
            return True, None

        # Construct INSERT SQL sentences.
        if col_list is None:
            attr_str = ''
        else:
            attr_str = '({})'.format(','.join(col_list))
        sql = "INSERT INTO {} {} VALUES".format(table, attr_str)

        value_pattern = "("
        for attr in res_list[0]:
            if type(attr) in [int, float, bool]:
                value_pattern += "{},"
            else:
                value_pattern += "'{}',"
        value_pattern = value_pattern[:-1] + '),'

        for res in res_list:
            sql += value_pattern.format(*res)
        sql = sql[:-1] + ';'
        sql = sql.replace('None', 'NULL')
        
        return self.execute(sql)
    
    def batch_delete(self, table, res_list, col_name):
        """
        Delete records from db multiple values of one column.

        :param table:       the name of table to perform DELETE
        :param res_list:    the 1d list values of column to DELETE
        :param col_name:    the column to define WHERE to DELETE
        """

        if len(res_list) == 0:
            return True, None

        # construct DELETE SQL sentences.
        sql = "DELETE FROM {} WHERE ".format(table)

        if type(res_list[0]) == int:
            condition_pattern = col_name + " = {}"
        else:
            condition_pattern = col_name + " = '{}'"
        
        sql += ' OR '.join([condition_pattern.format(res) for res in res_list]) + ';'
        
        return self.execute(sql)
    
    def dump(self, table):
        """
        Dump the given table's data.
        """
        dump_sql = "DELETE FROM {};".format(table)
        return self.execute(dump_sql)
