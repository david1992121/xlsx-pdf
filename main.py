import logging, os, configparser, glob, shutil, sys, time
from pdf_changer import PDFChanger
import pandas as pd
from threading import Thread
from logging.handlers import TimedRotatingFileHandler
from sql_follower import MssqlFollower
from multiprocessing import Process, JoinableQueue

def init_log():
    # set rotate log files
    cur_dir = os.getcwd()
    file_handler = TimedRotatingFileHandler(os.path.join(cur_dir, "logs/log"), when = 'H', interval=1, backupCount=7, encoding='utf8')
    file_handler.suffix = "%Y%m%d"
    logging.basicConfig(level=logging.INFO,
        format='MainProc %(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=(file_handler,),
    )

def empty_folder(folder):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(e)
            raise('Failed to delete %s'.format(file_path))

def get_category(tool_name):
    if "." in tool_name:
        return tool_name.split(".")[0]
    else:
        return tool_name

def get_program_data(sheet_data, cur_dir):
    program_data = {}
    program_data["ONumber"] = sheet_data.iloc[2][2] if not pd.isnull(sheet_data.iloc[2][2]) else ""
    program_data["ModelNum"] = sheet_data.iloc[3][2] if not pd.isnull(sheet_data.iloc[3][2]) else ""
    program_data["PartsName"] = sheet_data.iloc[0][7] if not pd.isnull(sheet_data.iloc[0][7]) else ""
    program_data["GoodsName"] = sheet_data.iloc[1][7] if not pd.isnull(sheet_data.iloc[1][7]) else ""
    program_data["FilesName"] = sheet_data.iloc[2][7] if not pd.isnull(sheet_data.iloc[2][7]) else ""
    program_data["CreateDate"] = sheet_data.iloc[0][16].strftime("%Y/%m/%d") if not pd.isnull(sheet_data.iloc[0][16]) else ""
    program_data["ItemCode"] = sheet_data.iloc[0][12] if not pd.isnull(sheet_data.iloc[0][12]) else ""
    program_data["Tools"] = sheet_data.iloc[1][12] if not pd.isnull(sheet_data.iloc[1][12]) else 0
    program_data["Creator"] = sheet_data.iloc[1][16] if not pd.isnull(sheet_data.iloc[1][16]) else ""
    program_data["Tooling"] = sheet_data.iloc[3][7] if not pd.isnull(sheet_data.iloc[3][7]) else ""
    program_data["ProcessTime"] = sheet_data.iloc[2][12] if not pd.isnull(sheet_data.iloc[2][12]) else ""
    program_data["FolderPath"] = cur_dir

    return program_data

def get_tooling_data(sheet_data, cur_dir):
    cur_index = 5
    tool_data = []
    rows_num = len(sheet_data.index)
    while cur_index < rows_num and not pd.isnull(sheet_data.iloc[cur_index][0]):
        cur_tool_data = {}
        cur_tool_data["ONumber"] = sheet_data.iloc[2][2] if not pd.isnull(sheet_data.iloc[2][2]) else ""
        cur_tool_data["ItemCode"] = sheet_data.iloc[0][12] if not pd.isnull(sheet_data.iloc[0][12]) else ""
        cur_tool_data["FilesName"] = sheet_data.iloc[2][7] if not pd.isnull(sheet_data.iloc[2][7]) else ""
        cur_tool_data["CreateDate"] = sheet_data.iloc[0][16].strftime("%Y/%m/%d") if not pd.isnull(sheet_data.iloc[0][16]) else ""
        cur_tool_data["Tooling"] = sheet_data.iloc[3][7] if not pd.isnull(sheet_data.iloc[3][7]) else ""
        cur_tool_data["FolderPath"] = cur_dir
        cur_tool_data["TNumber"] = sheet_data.iloc[cur_index][0]
        cur_tool_data["ToolName"] = sheet_data.iloc[cur_index][8]
        cur_tool_data["HolderName"] = sheet_data.iloc[cur_index][9]
        cur_tool_data["CutDistance"] = sheet_data.iloc[cur_index][17]
        tool_data.append(cur_tool_data)
        cur_index += 1        
    return tool_data

def pdf_proc(queue, error_queue, index, visible):
    pdf_changer = PDFChanger(queue, error_queue, index, visible)
    pdf_changer.execute_changing()

def error_proc(error_queue):
    cur_folder = os.path.dirname(__file__)
    err_file = os.path.join(cur_folder, 'fail_list.txt')
    while True:
        error_name = error_queue.get()        
        f = open(err_file, "a+")
        f.write("{}\n".format(error_name))
        f.close()
        error_queue.task_done()

def main(mode = "success"):
    
    init_log()
    logging.info("------------------------ スタート ------------------------")
    config = configparser.ConfigParser()
    config.read('setting.ini', 'utf-8')
    
    input_path = config["input"]["folder_path"]
    output_path = config["output"]["folder_path"]

    visible = config["pdf"]["visible"] == "true"
    proccesses = int(config["pdf"]["proccesses"])
    
    os.makedirs(output_path, exist_ok=True)

    file_queue = JoinableQueue()
    error_queue = JoinableQueue()
    mssql_follower = MssqlFollower(config)
    
    fail_list = []
    cur_folder = os.path.dirname(__file__)
    err_file = os.path.join(cur_folder, 'fail_list.txt')
    if mode == "fail":
        with open(err_file) as f:
            line = f.readline()
            while line:
                fail_list.append(line.strip())
                line = f.readline()
        os.rename(err_file, os.path.join(cur_folder, 'fail_list_origin.txt'))

    # print(fail_list)
    
    # start pdf proc
    procs = []
    for i in range(proccesses):
        pdf_changer_proc = Process(target = pdf_proc, args=(file_queue, error_queue, i, visible))
        pdf_changer_proc.start()
        procs.append(pdf_changer_proc)

    # start the error logging proc
    error_detector_proc = Process(target = error_proc, args=(error_queue, ))
    error_detector_proc.start()
    
    for file_item in glob.glob(os.path.join(input_path, '*.xlsx')):

        base_name = os.path.basename(file_item)
        file_name = os.path.splitext(base_name)[0]
        # print(base_name, file_name)

        if mode == "fail":
            if file_name not in fail_list:
                continue

        logging.info("現在の作業ファイル : {0}".format(os.path.basename(file_item)))        

        # get data from xlsx
        logging.info("Xlsxファイルからデータ取得中。。。：{}".format(file_item))
        main_file_data = pd.read_excel(file_item, "工具リスト", index_col=None, header=None)

        # get category name
        tool_name = main_file_data.iloc[3][7]
        category = get_category(tool_name)
        if category == "":
            logging.info("{0}のファイルが加工機名がありません。".format(base_name))
            continue

        category_path = os.path.join(output_path, category)
        # print(category)

        # if directory does not exist then make a new one
        if not os.path.exists(category_path):
            os.mkdir(category_path)

        cur_o_dir = os.path.join(category_path, file_name)
        # print(cur_o_dir)

        if not os.path.exists(cur_o_dir):
            os.mkdir(cur_o_dir)
        else:
            empty_folder(cur_o_dir)
        shutil.copy(file_item, cur_o_dir)
        
        # get mssql data
        program_data = get_program_data(main_file_data, cur_o_dir)
        if program_data["ONumber"] == "":
            logging.info("{0}のファイルがO番号がありません。".format(base_name))
            continue
        program_id = mssql_follower.set_program_data(program_data)

        tool_data = get_tooling_data(main_file_data, cur_o_dir)
        if len(tool_data) > 0:
            mssql_follower.set_tooling_data(tool_data, program_data["ONumber"], program_data["Tooling"], program_id)

        # xlsx to pdf
        file_queue.put({
            "input": file_item,
            "output": "{0}.pdf".format(os.path.join(cur_o_dir, file_name))
        })
        # break

    for _ in range(proccesses):
        file_queue.put({
            "input": "quit",
            "output": ""
        })

    logging.info("PDF化の処理完了待機中。。。")
    file_queue.join()

    time.sleep(1)
    logging.info("エラーリスト作成完了待機中。。。")
    error_queue.join()

    logging.info("-------------------- 処理完了 ----------------------")
    error_detector_proc.kill()

if __name__ == "__main__":
    if len(sys.argv) == 1:
        main()
    else:
        main(sys.argv[1])