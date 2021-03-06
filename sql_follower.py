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

    def set_program_data(self, program_data):
        try:
            logging.info("O番号:{0}, 加工機:{1}のプログラムレコード削除中。。。".format(program_data["ONumber"], program_data["Tooling"]))
            cursor = self.conn.cursor()
            cursor.execute(
                "DELETE Toolings_list FROM Toolings_list JOIN Programs_list ON Toolings_list.ProgramID = Programs_list.ID " + 
                "WHERE Programs_list.ONumber = '{0}' AND Programs_list.Tooling = '{1}'".format(program_data["ONumber"], program_data["Tooling"]))
            cursor.execute("DELETE FROM Programs_list WHERE ONumber = '{0}' AND Tooling = '{1}'".format(program_data["ONumber"], program_data["Tooling"]))
            self.conn.commit()

            cur_data = tuple(program_data.get(field_item, None) for field_item in program_data.keys())
            fields = [x for x in program_data.keys()]
            query = "INSERT INTO Programs_list({0}) VALUES ({1})".format( ",".join(fields), ",".join(["%s"] * len(fields)))
            cursor.execute(query, cur_data)
            self.conn.commit()
            logging.info("Programデータ追加完了")
            return cursor.lastrowid

        except Exception as e:
            logging.error("プログラムレコード操作中エラー発生")
            logging.error(e)
            self.conn.rollback()
            return 0

    def set_tooling_data(self, tool_data, program_id):
        try:
            logging.info("ProgramIDが {}のツールレコード削除中。。。".format(program_id))
            cursor = self.conn.cursor()
            # cursor.execute("DELETE FROM Toolings_list WHERE ProgramID = {}".format(program_id))
            # self.conn.commit()

            first_data = tool_data[0]
            data = []
            for tool_data_item in tool_data:
                data.append((program_id, ) + tuple(tool_data_item.get(field_item, None) for field_item in tool_data_item.keys()))
            fields = [x for x in first_data.keys()]
            fields.insert(0, 'ProgramID')
            query = "INSERT INTO Toolings_list({0}) VALUES ({1})".format( ",".join(fields), ",".join(["%s"] * len(fields)))
            cursor.executemany(query, data)

            # for data_item in data:
            #     print(query)
            #     print(data_item)
            #     cursor.execute(query, data_item)
            # print("no error")

            self.conn.commit()
            logging.info("Programデータ追加完了")
            return True

        except Exception as e:
            self.conn.rollback()
            logging.error("ツールレコード操作中エラー発生")
            logging.error(e)
            return False

    def __del__(self):
        logging.info("MSSQLフォロワープロセス終了。。。")