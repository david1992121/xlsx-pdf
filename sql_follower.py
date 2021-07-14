import pymssql, os, logging
from logging.handlers import TimedRotatingFileHandler

class MssqlFollower:
    def __init__(self, config):
        if not 'mssql' in config.keys():
            raise Exception("Setting.iniファイルにMSSQL設定がありません")
        
        self.init_log()
        self.connectToMssql(config)        

    def init_log(self):
        logpath = os.path.join(os.path.dirname(__file__), 'logs')
        if not os.path.exists(logpath):
            os.makedirs(logpath)
        follower_path = os.path.join(logpath, 'follower')
        if not os.path.exists(follower_path):
            os.mkdir(follower_path)

        logname = "log"

        # set rotate log files
        file_handler = TimedRotatingFileHandler(os.path.join(follower_path, logname), when = 'H', interval=1, backupCount=7, encoding='utf8')
        file_handler.suffix = "%Y%m%d%H00"
        logging.basicConfig(
            level=logging.INFO,
            format='フォロワープ %(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            handlers=(file_handler,),
        )
    
    def connectToMssql(self, config):
        try:
            host = config['mssql']['host']
            db_name = config['mssql']['db']
            user = config['mssql']['user']
            pwd = config['mssql']['password']
            port = config['mssql']['port']            

            logging.info("MSSQLサーバー {0}:{1}へ接続中。。。".format(host, port))
            self.conn = pymssql.connect(
                host,
                user,
                pwd,
                db_name
            )
            logging.info("MSSQLサーバーへ接続成功")

        except Exception as e:
            logging.error("MSSQLサーバーへ接続中エラー発生：{0}".format(e))
            raise Exception("MSSQL接続エラー")

    def startFollowing(self):
        logging.info("MongoDBデータベースの変化待機中。。。")
        while True:
            obj = self.queue.get()
            logging.info("MongoDBデータベース変化")
            logging.info("コレクション名：「{0}」".format(obj['collection']))
            logging.info("操作タイプ：{0}".format(obj["optype"]))
            logging.info("オブジェクトID：{0}".format(obj["_id"]))
            logging.info("処理データ: {}".format(obj["data"]))

            # process with data
            logging.info("データ反映中。。。")
            try:
                if obj["optype"] == "delete":
                    delete_id = obj["_id"]
                    self.remove_by_object_id(self.table_names[obj["collection"]], delete_id)
                elif obj["optype"] == "update":
                    updated_data = obj["data"]
                    update_array = []
                    for key_item in updated_data.keys():
                        key_field = self.key_dict[obj['collection']][key_item]
                        key_value = updated_data[key_item]
                        if isinstance(key_value, str):
                            update_array.append("{0} = N'{1}'".format(key_field, key_value))
                        elif isinstance(key_value, bool):
                            update_array.append("{0} = {1}".format(key_field, int(key_value)))
                        else:
                            update_array.append("{0} = {1}".format(key_field, key_value))
                    if len(update_array) > 0:
                        mssql_update_query = "UPDATE {0} set {1} WHERE ObjectID = '{2}'".format(
                            self.table_names[obj["collection"]], ", ".join(update_array), obj["_id"])
                        self.update_by_object_id(mssql_update_query)
                elif obj["optype"] == "insert":
                    updated_data = obj["data"]
                    updated_data["ObjectID"] = str(obj["_id"])
                    key_array = []                    
                    for key_item in updated_data.keys():
                        if key_item == "_id":
                            continue
                        if key_item == "ObjectID":
                            key_array.append("ObjectID")
                        else:
                            key_array.append(self.key_dict[obj['collection']][key_item])

                    value_array = tuple(updated_data.get(key_item, None) for key_item in updated_data.keys() if key_item != "_id")
                        
                    if len(key_array) > 0:
                        if len(key_array) == len(value_array):
                            self.insert_one_record(self.table_names[obj['collection']], key_array, value_array)
                        else:
                            raise Exception("データ追加フィールド数と値の数が一致しません")
            except Exception as e:
                logging.error("データ反映中エラー発生")
                logging.error(e)        
    
    def drop_table(self, table_name):
        try:
            logging.info("「{}」テーブルを削除中。。。".format(table_name))
            cursor = self.conn.cursor()
            cursor.execute("IF OBJECT_ID(N'{0}', N'U') IS NOT NULL DROP TABLE {1}".format(table_name, table_name))
            self.conn.commit()
            logging.info("「{}」テーブルを削除完了".format(table_name))
        except Exception as e:
            logging.error("「{}」テーブルを削除中エラー発生".format(table_name))
            logging.error(e)
            raise Exception("「{}」テーブルのデータ削除中にエラー".format(table_name))
    
    def create_table(self, table_name):
        try:
            logging.info("「{}」テーブル作成中。。。".format(table_name))
            cur_folder = os.path.dirname(__file__)
            cursor = self.conn.cursor()
            with open(os.path.join(cur_folder, "queries/{}.sql".format(table_name)), "r") as f:
                data = ''.join(line for line in f)
                cursor.execute(data)
                self.conn.commit()
            logging.info("「{}」テーブル作成完了".format(table_name))
        except Exception as e:
            logging.error("「{}」テーブルの作成中エラー発生".format(table_name))
            logging.error(e)
            raise Exception("「{}」テーブルの作成中エラー".format(table_name))


    def recreate_table(self, table_name):
        self.drop_table(table_name)
        self.create_table(table_name)

    def insert_data(self, table_name, fields, data):
        try:
            logging.info("「{}」テーブルにデータ追加中。。。".format(table_name))
            cursor = self.conn.cursor()
            query = "INSERT INTO {0}({1}) VALUES ({2})".format(table_name, ",".join(fields), ",".join(["%s"] * len(fields)))
            cursor.executemany(query, data)
            self.conn.commit()
            logging.info("「{}」テーブルにデータ追加完了".format(table_name))
        except Exception as e:
            logging.error("「{}」テーブルにデータ追加中エラー発生".format(table_name))
            logging.error(e)
            raise Exception("「{}」テーブルにデータ追加中エラー".format(table_name))

    def remove_by_object_id(self, table_name, object_id):
        try:
            logging.info("「{0}」テーブルでObjectID「{1}」のレコード削除中。。。".format(table_name, object_id))
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM {0} WHERE ObjectID = '{1}'".format(table_name, object_id))
            self.conn.commit()
            logging.info("削除成功")
        except Exception as e:
            logging.info("削除中エラー発生：{}".format(e))

    def update_by_object_id(self, query):
        try:
            logging.info("アップデートクエリ：{}".format(query))
            cursor = self.conn.cursor()
            cursor.execute(query)
            self.conn.commit()
            logging.info("更新成功")
        except Exception as e:
            logging.info("更新中エラー発生：{}".format(e))

    def insert_one_record(self, table_name, fields, data):
        try:
            logging.info("「{}」テーブルにレコード追加中。。。".format(table_name))
            cursor = self.conn.cursor()
            query = "INSERT INTO {0}({1}) VALUES ({2})".format(table_name, ",".join(fields), ",".join(["%s"] * len(fields)))
            cursor.execute(query, data)
            self.conn.commit()
            logging.info("「{}」テーブルにレコード追加完了".format(table_name))
        except Exception as e:
            logging.error("「{}」テーブルにレコード追加中エラー発生".format(table_name))
            logging.error(e)
            raise Exception("「{}」テーブルにレコード追加中エラー".format(table_name))

    def __del__(self):
        logging.info("MSSQLフォロワープロセス終了。。。")